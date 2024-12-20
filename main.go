package main

import (
	"encoding/json"
	"io"
	"log"
"time"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// API Links
const (
	baseURL       = "https://pricing.us-east-1.amazonaws.com"
	regionURL     = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/region_index.json"
	dbConnStr     = "host=localhost user=postgres password=password dbname=cco sslmode=disable"
	priceListPath = "./price-list"
)

// Models
type Provider struct {
	ID           uint   `gorm:"primaryKey"`
	ProviderName string `gorm:"unique"`
}
type Service struct {
	ID          uint   `gorm:"primaryKey"`
	ServiceName string
	ProviderID  uint
}
type Region struct {
	ID         uint   `gorm:"primaryKey"`
	RegionCode string `gorm:"unique"`
	ServiceID  uint
}
type Product struct {
	SKU          string            `json:"sku"`
	ProductFamily string           `json:"productFamily"`
	Attributes    map[string]string `json:"attributes"`
} 
type SKU struct {
	ID              uint   `gorm:"primaryKey"`
	SKUCode         string `gorm:"unique"`
	ProductFamily   string
	VCPU            int
	OperatingSystem string
	InstanceType    string
	Storage         string
	Network         string
	InstanceSKU     string
	Processor       string
	UsageType       string
	RegionID        uint
}
// Define Term struct for terms table
type Term struct {
    OfferTermID          int       `gorm:"primaryKey;autoIncrement"`
    SKU_ID               int       `gorm:"not null"`
    OfferTermCode        string    `gorm:"not null"`
    LeaseContractLength  string
    PurchaseOption       string
    OfferingClass        string
    CreatedAt            time.Time `gorm:"default:CURRENT_TIMESTAMP"`
    ModifiedAt           time.Time `gorm:"default:CURRENT_TIMESTAMP"`
    DisableFlag          bool      `gorm:"default:false"`
}
// Update TermDetails struct to include termAttributes
type TermDetails struct {
    SKU             string            `json:"sku"`
    OfferTermCode   string            `json:"offerTermCode"`
    TermAttributes  TermAttributes    `json:"termAttributes"`
    PriceDimensions map[string]PriceDimension `json:"priceDimensions"`
}
// Define TermAttributes struct
type TermAttributes struct {
    LeaseContractLength string `json:"LeaseContractLength"`
    PurchaseOption      string `json:"PurchaseOption"`
	 OfferingClass       string `json:"OfferingClass"`
}
// Term struct for the "terms" table
type PriceDimension struct {
    PricePerUnit map[string]string `json:"pricePerUnit"`
    RateCode     string            `json:"rateCode"`
    Description  string            `json:"description"`
    BeginRange   string            `json:"beginRange"`
    EndRange     string            `json:"endRange"`
    Unit         string            `json:"unit"`
}
type PricingData struct {
    Products map[string]Product `json:"products"`
    Terms    map[string]map[string]map[string]TermDetails `json:"terms"`
}



// Process current version 
func processCurrentVersionFile(db *gorm.DB, filepath string, regionID uint) {
    file, err := os.Open(filepath)
    if err != nil {
        log.Fatalf("Failed to open current version file: %v", err)
    }
    defer file.Close()

    var data PricingData

    err = json.NewDecoder(file).Decode(&data)
    if err != nil {
        log.Fatalf("Failed to decode current version file: %v", err)
    }

    // Convert map to slice and call function to process products (SKUs)
    productsSlice := mapToSlice(data.Products)
    processProducts(db, productsSlice, regionID)

    // Call function to process terms
	

	processTerms(db, data.Terms["OnDemand"]) 
}
// Helper function to convert map to slice
func mapToSlice(productsMap map[string]Product) []Product {
    productsSlice := make([]Product, 0, len(productsMap))
    for _, product := range productsMap {
        productsSlice = append(productsSlice, product)
    }
    return productsSlice
}
// Function to process and insert products (SKUs) into the DB
func processProducts(db *gorm.DB, products []Product, regionID uint) {
    for _, product := range products {
        // Check and parse VCPU, default to 0 if missing
        vcpu, _ := strconv.Atoi(defaultIfEmpty(product.Attributes["vcpu"], "0"))

        // Create SKU record
        sku := SKU{
            SKUCode:         product.SKU,
            ProductFamily:   product.ProductFamily,
            VCPU:            vcpu,
            OperatingSystem: product.Attributes["operatingSystem"],
            InstanceType:    product.Attributes["instanceType"],
            Storage:         product.Attributes["storage"],
            Network:         product.Attributes["networkPerformance"],
            InstanceSKU:     product.Attributes["instancesku"],
            Processor:       product.Attributes["physicalProcessor"],
            UsageType:       product.Attributes["usagetype"],
            RegionID:        regionID,
        }

        // Insert SKU (check if it exists, create if not)
        if err := db.FirstOrCreate(&sku, SKU{SKUCode: sku.SKUCode}).Error; err != nil {
            log.Printf("Failed to insert SKU %s: %v", product.SKU, err)
        } else {
            log.Printf("Successfully inserted SKU: %s", product.SKU)
        }
    }
}

func processTerms(db *gorm.DB, terms map[string]map[string]TermDetails) {
    for skuCode, termData := range terms {
        log.Printf("Processing term type: %s", skuCode)

        for termType, termDetails := range termData {
            log.Printf("Processing SKU: %s", termType)

            // Find sku_id for the given skuCode
            var skuID int
            if err := db.Table("skus").Select("id").Where("sku_code = ?", skuCode).Scan(&skuID).Error; err != nil {
                log.Printf("Failed to find sku_id for SKU %s: %v", skuCode, err)
                continue
            }
			log.Printf("Skuid %v", skuID)

            // Create term entry
            termEntry := Term{
                SKU_ID:               skuID,
                OfferTermCode:        termDetails.OfferTermCode,
                LeaseContractLength:  termDetails.TermAttributes.LeaseContractLength,
                PurchaseOption:       termDetails.TermAttributes.PurchaseOption,
                OfferingClass:        termDetails.TermAttributes.OfferingClass,
            }

            // Insert term entry into the database
            if err := db.Create(&termEntry).Error; err != nil {
                log.Printf("Failed to insert term for SKU %s: %v", skuCode, err)
            } else {
                log.Printf("Successfully inserted term for SKU %s", skuCode)
            }
        }
    }
}
// Helper function to handle empty values
func defaultIfEmpty(value, defaultValue string) string {
    if value == "" {
        return defaultValue
    }
    return value
}




























//function to download files from a URL
func downloadFile(url, filepath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
func main() {
	// Initialize DB connection
	db, err := gorm.Open(postgres.Open(dbConnStr), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}

	// Auto-migrate schemas
	db.AutoMigrate(&Provider{}, &Service{}, &Region{}, &SKU{}, &Term{})

	// Create directories for storing price list files
	err = os.MkdirAll(priceListPath, os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create price-list directory: %v", err)
	}

	// Download region index file
	regionFilePath := filepath.Join(priceListPath, "region_index.json")
	err = downloadFile(regionURL, regionFilePath)
	if err != nil {
		log.Fatalf("Failed to download region: %v", err)
	}

	// Open and parse region index file
	regionFile, err := os.Open(regionFilePath)
	if err != nil {
		log.Fatalf("Failed to open region file: %v", err)
	}
	defer regionFile.Close()

	var regionData struct {
		Regions map[string]struct {
			RegionCode       string `json:"regionCode"`
			CurrentVersionUrl string `json:"currentVersionUrl"`
		} `json:"regions"`
	}
	err = json.NewDecoder(regionFile).Decode(&regionData)
	if err != nil {
		log.Fatalf("Failed to decode region index file: %v", err)
	}

	// Initialize Provider and Service
	provider := Provider{ProviderName: "AWS"}
	db.FirstOrCreate(&provider, Provider{ProviderName: "AWS"})

	service := Service{ServiceName: "AmazonEC2", ProviderID: provider.ID}
	db.FirstOrCreate(&service, Service{ServiceName: "AmazonEC2"})

	// Process each region
	for _, region := range regionData.Regions {
		log.Printf("Processing region: %s", region.RegionCode)

		// Insert Region data into DB
		regionEntry := Region{
			RegionCode: region.RegionCode,
			ServiceID:  service.ID,
		}
		db.FirstOrCreate(&regionEntry, Region{RegionCode: region.RegionCode})

		// Download the current version file for each region
		currentVersionURL := baseURL + region.CurrentVersionUrl
		currentVersionFile := filepath.Join(priceListPath, region.RegionCode+".json")
		err = downloadFile(currentVersionURL, currentVersionFile)
		if err != nil {
			log.Printf("Failed to download %s: %v", currentVersionURL, err)
			continue
		}

		// Process the pricing data in the current version file
		processCurrentVersionFile(db, currentVersionFile, regionEntry.ID)

		// Remove the downloaded file after processing
		err = os.Remove(currentVersionFile)
		if err != nil {
			log.Printf("Failed to delete file %s: %v", currentVersionFile, err)
		} else {
			log.Printf("Successfully deleted file: %s", currentVersionFile)
		}
	}
}