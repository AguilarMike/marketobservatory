#| echo: false
cat("\014")  # clear console
rm(list=ls())  # Clear the workspace

# ---- SETUP ----
library(tidyverse)
library(tidyquant)
library(httr)
library(jsonlite)
library(bea.R)
library(dplyr)
library(stringr)
library(knitr)
library(readxl)
library(tibble)
library(openxlsx)
library(tidyr)
library(here)

#Local
# ---- Load API key ----
#beaKey <- readLines("./supporting/API.txt")[1]
# ---- Load historical data ----
#CorpIncome_Annual_Data <- read_csv("./Data/CorpIncome-Annual-Data.csv")
#load("./Data/CorpIncome-Annual-Data.Rdata")

# Git 
# ---- Load API key ----
beaKey <- readLines(here("scripts", "BEA_API.txt"))[1]
# ---- Load historical data ----
CorpIncome_Annual_Data <- read_csv(here("data","CorpIncome-Annual-Data.csv"))
load(here::here("data", "CorpIncome-Annual-Data.RData"))


# ---- GET NEW DATA USING BEA API ----
# ---- NIPA ----
# ---- Prepare to loop ----
# Filter for NIPA tables only and get unique TableIDs
nipa_metadata <- metadata %>%
  filter(Database == "NIPA") 

nipa_ids <- unique(nipa_metadata$TableID)

# Initialize an empty list to store the raw data pulled from each table
NIPA_df <- list()

# ---- Looping ----
for (id in nipa_ids) {
  
  # Set up the call
  beaSpec_NIPA <- list(
    'UserID' = beaKey,
    'Method' = 'GetData',
    'datasetname' = 'NIPA',
    'TableName' = id,
    'Frequency' = 'A',
    'Year' = 'All',
    'ResultFormat' = 'json'
  )
  
  # Pull data using beaGet()
  NIPA_df[[id]] <- beaGet(beaSpec_NIPA)
}



# ---- Extract required lines and rewrite the entire database ----
 # Matching LineDescription in meta data with the same LineDescription in NIPA database
extract_var_long <- function(var, metadata, df_list) {
  rows <- metadata %>% filter(VariableName == var)
  map2_dfr(rows$TableID, seq_len(nrow(rows)), function(tbl, rowidx) {
    desc <- rows$LineDescription[rowidx]
    ln   <- rows$LineNumber[rowidx]
    # Normalize description (lowercase, remove all whitespace)
    norm_desc <- function(x) str_replace_all(tolower(x), "\\s+", "")
    target_desc <- norm_desc(desc)
    df <- df_list[[tbl]]
    df <- df %>%
      mutate(LineDescription_norm = norm_desc(LineDescription), LineNumber = as.integer(LineNumber)) %>%
      filter(LineDescription_norm == target_desc, LineNumber == ln) %>%
      select(starts_with("DataValue_")) %>%
      pivot_longer(everything(), names_to = "Date", values_to = var) %>%
      mutate(Date = str_remove(Date, "DataValue_"))
  }) %>%
    group_by(Date) %>%
    reframe(!!var := coalesce(!!!syms(var)))
}

var_names <- unique(nipa_metadata$VariableName)

 # For each variable, create a long format (Date, Value) table
var_long_list <- map(var_names, extract_var_long, metadata = nipa_metadata, df_list = NIPA_df)

 # Reduce to one wide data.frame
NIPA_data <- reduce(var_long_list, full_join, by = "Date") %>%
  arrange(Date)

 # Clean the data
num_vars <- setdiff(names(NIPA_data), "Date")
CorpIncome_Annual_Data <- NIPA_data %>%
  group_by(Date) %>%
  summarise(across(all_of(num_vars), ~ coalesce(.[!is.na(.)][1], NA_real_)), .groups = "drop")


  # Update calculated data
  CorpIncome_Annual_Data <- CorpIncome_Annual_Data %>%
    mutate(
      Corp.OpEx = Corp.OpEx.EmpComp + Corp.OpEx.TaxOnProductionLessSubsidy ,
      Corp.PayoutRatio = Corp.Dividends/Corp.NetIncome.NonGAAP ,
      Corp.RetentionRatio = 1 - (Corp.Dividends/Corp.NetIncome.NonGAAP),
      Corp.TaxRate = Corp.IncomeTax/Corp.NetIncome.NonGAAP,
    )
  # -- For the wide data.R --
 CorpIncome.Annual.Data.Wide <- CorpIncome_Annual_Data
  
  # -- For the Long data.R --
 CorpIncome.Annual.Data.Long <-CorpIncome.Annual.Data.Wide %>%
    mutate(across(-Date, as.character)) %>%
    pivot_longer(-Date, names_to = "variable", values_to = "value")
 
 CorpIncome.Annual.Data.Long <- CorpIncome.Annual.Data.Long %>%
   mutate(
     value = gsub(",", "", value),          
     value = gsub("\\$", "", value),       
     value = na_if(value, ""),         
     value = as.numeric(value) 
   ) %>%
   arrange(Date) %>%
   group_by(variable) %>%
   mutate(delta = (value - lag(value)) / lag(value))
 
 #Local
# ----Save the updated Data----
 #write.csv(CorpIncome_Annual_Data, "Data/CorpIncome-Annual-Data.csv", row.names = FALSE)
 #save(metadata, CorpIncome.Annual.Data.Wide, CorpIncome.Annual.Data.Long, file = "Data/CorpIncome-Annual-Data.RData")
 
 # Git
 write.csv(CorpIncome_Annual_Data,  here("data", "CorpIncome-Annual-Data.csv"),   row.names = FALSE)
 save(metadata,CorpIncome.Annual.Data.Wide, CorpIncome.Annual.Data.Long, file = here("data","CorpIncome-Annual-Data.RData"))
