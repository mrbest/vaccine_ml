options(scipen = 999)
source("sprklyRSpark.R")
library(readr)
library(ggplot2)
library(tictoc)
library(lucr)


process_gsa_contracts <- function()
{ #declare vector accumulators for contract name, addressable obligations result and contract actual obligations 
  actual_obligations_vector <- double()
  addressable_market_vector <- double()
  contract_name_vector <- character()
  #get dummy addressable matric for master addressable matrix creation
  master_addressability_matrix <- dplyr_gen_addressability_matrix_df("OS3", training_transactions)
  #count the rows and make negative in prep for deletion
  rowcount <- master_addressability_matrix %>% count() %>% .$n 
  rowcount <- rowcount * -1
  #set master_addressability_matrix up for recieving addressability matrices
  master_addressability_matrix <<- master_addressability_matrix[-1:rowcount, ]
  
  #declare and query list of GSA contracts
  gsa_contracts <- training_transactions %>% 
    filter(managing_agency == "GSA") %>% 
    distinct(contract_name) %>% 
    collect() %>% .$contract_name
    gsa_contracts <- na.omit(gsa_contracts)
  #set up sentinel for looping through GSA contracts  
  contract_count <- length(gsa_contracts)
  for(i in 1:contract_count)
    {##accumulate addressable market and actual obligations contract by contract
     addressable_market_vector <- append(addressable_market_vector, process_one_contract(gsa_contracts[i]))
     contract_name_vector <- append(contract_name_vector, gsa_contracts[i])
     actual_obligations_vector <- append(actual_obligations_vector, opt_get_contract_totals(gsa_contracts[i]))
     }
  #write result for all contracts to a dataframe  
  gsa_result_df <- data_frame(contract_name_vector, actual_obligations_vector, addressable_market_vector)
  gsa_result_df
}

process_bic_contracts <- function()
{
  #declare vector accumulators for contract name, addressable obligations result and contract actual obligations 
  actual_obligations_vector <- double()
  addressable_market_vector <- double()
  contract_name_vector <- character()
  #get dummy addressable matric for master addressable matrix creation
  master_addressability_matrix <- dplyr_gen_addressability_matrix_df("OS3", training_transactions)
  #count the rows and make negative in prep for deletion
  rowcount <- master_addressability_matrix %>% count() %>% .$n 
  rowcount <- rowcount * -1
  #set master_addressability_matrix up for recieving addressability matrices
  master_addressability_matrix <<- master_addressability_matrix[-1:rowcount, ]
  #declare and query list of BIC contracts
  #STEP 1. Query official_bic_contract column for distinct BICs
  bic_contract_list <- training_transactions %>% 
    distinct(official_bic_contract) %>% na.omit() %>% collect() %>% .$official_bic_contract
  #STEP 2. Query contract_name column for BICs. This has to be done because contract names are different in the 
  # official_bic_contract column from the names used in the contract column
  bic_contracts <<- training_transactions %>% 
      filter(official_bic_contract %in% bic_contract_list) %>% 
      distinct(contract_name) %>%
      collect() %>% 
     .$contract_name
  #set up sentinel for looping through GSA contracts 
  contract_count <- length(bic_contracts)
  for(i in 1:contract_count)
  { ##accumulate addressable market and actual obligations contract by contract
    addressable_market_vector <- append(addressable_market_vector, process_one_contract(bic_contracts[i]))
    contract_name_vector <- append(contract_name_vector, bic_contracts[i])
    actual_obligations_vector <- append(actual_obligations_vector, opt_get_contract_totals(bic_contracts[i]))
  }
  
  bic_result_df <- data_frame(contract_name_vector, actual_obligations_vector, addressable_market_vector)
  bic_result_df
}


process_one_contract <- function(contract_name)
{ 
  addressability_matrix <- dplyr_gen_addressability_matrix_df(contract_name, training_transactions)
  master_addressability_matrix <<- bind_rows(master_addressability_matrix, addressability_matrix)
  result_df <- dplyr_gen_testPhase_df(addressability_matrix, testing_transactions)
  addressability_result <- result_df %>% select(dollars_obligated)%>% sum()
  actual_obligations <<- opt_get_contract_totals(contract_name)
  addressability_result_formatted <- to_currency(addressability_result, currency_symbol = "$", symbol_first = TRUE, group_size = 3, group_delim = ",", decimal_size = 2,decimal_delim = ".")
  print(paste0( contract_name," addressable spend is : ", addressability_result_formatted))
  addressability_result
}

load_spark_csv <- function(sc, filename)
{##future dev note: file loading phase can be speeded up by using spark csv reader
  
  print("Reading in export")
  raw_df <<- spark_read_csv(sc, name = "sprkdf", filename,delimiter = "\t", header = TRUE, overwrite = TRUE)
  toc( )
  
  print("Performing socio-economic factor clean-up")
  ###Re-code NAs first!!!!!!!
  raw_df <<- raw_df %>% mutate(sbg_flag = if_else(is.na(sbg_flag) == TRUE, "FALSE", sbg_flag))
  raw_df <<- raw_df %>% mutate(women_owned_flag = if_else(is.na(women_owned_flag) == TRUE, "FALSE", women_owned_flag))
  raw_df <<- raw_df %>% mutate(veteran_owned_flag = if_else(is.na(veteran_owned_flag) == TRUE, "FALSE", veteran_owned_flag))
  raw_df <<- raw_df %>% mutate(minority_owned_business_flag = if_else(is.na(minority_owned_business_flag) == TRUE, "FALSE", minority_owned_business_flag))
  raw_df <<- raw_df %>% mutate(foreign_government = if_else(is.na(foreign_government) == TRUE, "FALSE", foreign_government))
  
  raw_df <<- raw_df %>% mutate(sbg_flag = if_else(sbg_flag=="Y", "SBG", "FALSE"))
  raw_df <<- raw_df %>% mutate(women_owned_flag = if_else(women_owned_flag == "YES", "WO", "FALSE"))
  raw_df <<- raw_df %>% mutate(veteran_owned_flag = if_else(veteran_owned_flag == "YES", "VO", "FALSE"))
  raw_df <<- raw_df %>% mutate(minority_owned_business_flag = if_else(minority_owned_business_flag == "YES", "MB", "FALSE"))
  raw_df <<- raw_df %>% mutate(foreign_government = if_else(foreign_government == "YES", "FG", "FALSE"))

  print("Creating add_key for all transactions")
  #filter by date range to only have FY16
  
  print("subsetting training transactions")
  training_transactions <<- raw_df %>% filter(as.Date(date_signed) >= as.Date("2013-10-01") & as.Date(date_signed) <= as.Date("2016-09-30"))
  
  
  print("subsetting testing transactions")
  testing_transactions <<- raw_df %>% filter(as.Date(date_signed) >= as.Date("2016-10-01") & as.Date(date_signed) <= as.Date("2017-09-30"))
  
}


load_spark_parquet <- function(archive)
{
  
  print("Reading in export")
  raw_df <<- spark_read_parquet(sc, name="raw_df", path = archive)
  #filter by date range to only have FY16
  
  
  print("Creating add_key for all transactions")
  
  
  print("subsetting training transactions")
  training_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2012-10-01") & as.Date(date_signed) <= as.Date("2017-09-30")) 
  training_transactions <<- training_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  
  
  print("subsetting testing transactions")
  fy13testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2012-10-01") & as.Date(date_signed) <= as.Date("2013-09-30")) 
  fy13testing_transactions <<- fy13testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  
  fy14testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2013-10-01") & as.Date(date_signed) <= as.Date("2014-09-30")) 
  fy14testing_transactions <<- fy14testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  
  fy15testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2014-10-01") & as.Date(date_signed) <= as.Date("2015-09-30")) 
  fy15testing_transactions <<- fy15testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  
  fy16testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2015-10-01") & as.Date(date_signed) <= as.Date("2016-09-30")) 
  fy16testing_transactions <<- fy16testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  
  fy17testing_transactions <- raw_df %>% filter(as.Date(date_signed) >= as.Date("2016-10-01") & as.Date(date_signed) <= as.Date("2017-09-30")) 
  fy17testing_transactions <<- fy17testing_transactions %>% mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  
  
  
}



opt_get_contract_totals <- function(contract_label)
{
  contract_total_obligations <- testing_transactions %>% 
    filter(contract_name == contract_label) %>% 
    select(dollars_obligated) %>% collect() %>% 
    sum()
  
  contract_total_obligations
}

dplyr_gen_addressability_matrix_df <- function(contract_label, training_df)
{
  #builds addressabbility matrix based on 6 factors
  addressability_matrix_df <-  training_df %>% filter(contract_name == contract_label) %>% 
    distinct( contract_name, product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government) %>%
    arrange( product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government) %>%
    collect()
  #adds addressability key to matrix post collection
  addressability_matrix_return <- addressability_matrix_df %>% 
    mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  addressability_matrix_return
}


dplyr_addressability_frequency_gen <- function(addkey_param)
{
## Generate frequency table corresponding to addressability factor entries 
## Each frequency entry corresponds to the frequency of in a specific addressability key entry shows up. 
## The outstanding question is in for a specific contract. 
## training_transactions must be of global scope
## function is meant for use within an apply statement
signature_count <- training_transactions %>% group_by(addkey) %>%
  summarise(total = n()) %>% arrange(total) %>% collect

}



dplyr_gen_testPhase_df <- function(addressability_matrix, testing_df)
{
  addressability_matrix_addkey <- addressability_matrix %>% select(addkey) %>% .$addkey
  addressability_test_result <- testing_df %>% 
                                #filter(level_1_category_group == "GWCM") %>%#
                                filter(addkey %in% addressability_matrix_addkey) %>% collect()
  addressability_test_result
}


addressability_injection <- function(addressability_matrix, inject_filename)
{
  #read in addressability rows. 
  signature_update <- read_csv(filename)
  #inject rows in to addressability matrix
  modified_addressability_matrix <- bind_rows(addressabiity_matrix, signature_update)
  #return updated addressability matrix
  modified_addressability_matrix
}


calc_frequency <-function(foriegn_addkey, transaction_df)
{
  total_rowcount <- transaction_df %>% count() %>% collect()
  count_matching_signature <- transaction_df %>% filter(addkey == foriegn_addkey) %>% count() %>%collect()
  proportion = count_matching_signature / total_rowcount
  proportion
}

frequency_threshold_filter <- function(addressability_matrix, transaction_df)
{
##pre: Access to addressability matrix and testing data set: 
##note: we may want to try using a time duration == to the training and testing data set to get the right frequency driven filtering
mod_add_matrix_matrix <- addressability_matrix %>%rowwise()%>% mutate(proportion = calc_frequency(addkey, transaction_df))
mod_add_matrix
}


capture_FAS_Training_Awards <- function(regex_pattern, start_date, end_date)
{
    fas_contracts <- raw_df %>% filter(as.Date(date_signed) >= as.Date(start_date) & as.Date(date_signed) <= as.Date(end_date)) %>%
    filter(rlike(reference_piid, regex_pattern) == TRUE) %>% 
    select(date_signed,reference_piid, idv_ref_idv_piid, managing_agency, contracting_agency_name, product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government, dollars_obligated)
}


capture_FAS_Dependent_BPA_Training_Awards <- function(regex_pattern, start_date, end_date)
{
     fas_bpas <- raw_df %>% filter(as.Date(date_signed) >= as.Date(start_date) & as.Date(date_signed) <= as.Date(end_date)) %>%
       filter(rlike(idv_ref_idv_piid, regex_pattern) == TRUE) %>% 
       select(date_signed,reference_piid, idv_ref_idv_piid, managing_agency, contracting_agency_name, product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government, dollars_obligated)
}


FAS_dplyr_gen_addressability_matrix_df <- function(regex_pattern)
{
  fas_awards <- capture_FAS_Training_Awards("^GS..[FKQT]", "2013-10-01", "2016-09-30")
  #builds addressabbility matrix based on 6 factors
  fas_ref_piids <- fas_awards%>% select(reference_piid) %>% distinct() %>% collect() %>% .$reference_piid
  addressability_matrix_df <-  raw_df %>% filter(as.Date(date_signed) >= as.Date("2013-10-01") & as.Date(date_signed) <= as.Date("2016-09-30")) %>%
    filter(reference_piid %in% fas_ref_piids) %>% 
    distinct( contract_name, product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government) %>%
    arrange( product_or_service_code, naics_code, sbg_flag, women_owned_flag, veteran_owned_flag, minority_owned_business_flag, foreign_government) %>%
    collect()
  #adds addressability key to matrix post collection
  addressability_matrix_return <- addressability_matrix_df %>% 
    mutate(addkey = paste0(product_or_service_code,"_",naics_code,"_", sbg_flag,"_", women_owned_flag,"_", veteran_owned_flag,"_", minority_owned_business_flag,"_", foreign_government) )
  addressability_matrix_return
}




