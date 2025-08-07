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
#CorpIncome_Quarterly_Data <- read_csv("./Data/CorpIncome-Quarterly-Data.csv")
#load("./Data/CorpIncome-Quarterly-Data.Rdata")

# Git 
# ---- Load API key ----
beaKey <- readLines(here("scripts", "BEA_API.txt"))[1]
# ---- Load historical data ----
CorpIncome_Quarterly_Data <- read_csv(here("data","CorpIncome-Quarterly-Data.csv"))
load(here::here("data", "CorpIncome-Quarterly-Data.RData"))


# ---- GET NEW DATA FROM BEA API ----

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
    'Frequency' = 'Q',
    'Year' = 'All',
    'ResultFormat' = 'json'
  )
  
  # Pull data using beaGet()
  NIPA_df[[id]] <- beaGet(beaSpec_NIPA)
}

# ---- Extract required lines and rewrite the entire database----
# Matching LineDescription in meta data with the same LineDescription in NIPA database
extract_var_long <- function(var, metadata, df_list) {
  rows <- metadata %>% filter(VariableName == var)
  norm_desc <- function(x) stringr::str_replace_all(tolower(x), "\\s+", "")
  map2_dfr(rows$TableID, seq_len(nrow(rows)), function(tbl, rowidx) {
    desc <- rows$LineDescription[rowidx]
    ln   <- rows$LineNumber[rowidx]
    df <- df_list[[tbl]]
    df %>%
      filter(
        norm_desc(LineDescription) == norm_desc(desc),
        as.integer(LineNumber) == ln
      ) %>%
      select(starts_with("DataValue_")) %>%
      pivot_longer(everything(), names_to = "Date", values_to = var) %>%
      mutate(Date = stringr::str_remove(Date, "DataValue_"))
  }) %>%
    group_by(Date) %>%
    reframe(!!var := coalesce(!!!syms(var)))
}

var_names <- unique(nipa_metadata$VariableName)

var_long_list <- map(var_names, extract_var_long, metadata = nipa_metadata, df_list = NIPA_df)

NIPA_data <- reduce(var_long_list, full_join, by = "Date") %>%
  arrange(Date)

num_vars <- setdiff(names(NIPA_data), "Date")

CorpIncome_Quarterly_Data <- NIPA_data %>%
  group_by(Date) %>%
  summarise(across(all_of(num_vars), ~ coalesce(.[!is.na(.)][1], NA_real_)), .groups = "drop")


# Update calculated data
CorpIncome_Quarterly_Data <- CorpIncome_Quarterly_Data %>%
  mutate(
    Corp.OpEx = Corp.OpEx.EmpComp + Corp.OpEx.TaxOnProductionLessSubsidy ,
    Corp.PayoutRatio = Corp.Dividends/Corp.NetIncome.NonGAAP ,
    Corp.RetentionRatio = 1 - (Corp.Dividends/Corp.NetIncome.NonGAAP),
    Corp.TaxRate = Corp.IncomeTax/Corp.NetIncome.NonGAAP,
  )

# -- For the wide data.R --
CorpIncome.Quarterly.Data.Wide <- CorpIncome_Quarterly_Data

# -- For the Long data.R --
CorpIncome.Quarterly.Data.Long <-CorpIncome.Quarterly.Data.Wide %>%
  mutate(across(-Date, as.character)) %>%
  pivot_longer(-Date, names_to = "variable", values_to = "value")

CorpIncome.Quarterly.Data.Long <- CorpIncome.Quarterly.Data.Long %>%
  mutate(DateNum = as.yearqtr(Date, format = "%YQ%q"),
         value = as.numeric(value)) %>%
  arrange(DateNum) %>%
  group_by(variable) %>%
  mutate(delta = (value - lag(value)) / lag(value))

#Local
# ----Save the updated Data----
#write.csv(CorpIncome_Quarterly_Data, "Data/CorpIncome-Quarterly-Data.csv", row.names = FALSE)
#save(metadata, CorpIncome.Quarterly.Data.Wide, CorpIncome.Quarterly.Data.Long, file = "Data/CorpIncome-Quarterly-Data.RData")


# Git
write.csv(CorpIncome_Quarterly_Data,  here("data", "CorpIncome-Quarterly-Data.csv"),   row.names = FALSE)
save(metadata,CorpIncome.Quarterly.Data.Wide, CorpIncome.Quarterly.Data.Long, file = here("data","CorpIncome-Quarterly-Data.RData"))
