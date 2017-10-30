source("sprklyRSpark.R")
source("opt_addressability.R")
library(knncat)

init <- function(){
sc <<- sparkInit()
load_spark_parquet("../../GWCM_Extracts/FY13_17_complete.prq/")
fy13_17_hhs_6505 <<- raw_df %>% 
  filter(product_or_service_code == "6505") %>% 
  filter(funding_department_name == "HEALTH AND HUMAN SERVICES, DEPARTMENT OF") %>%
  collect()

  fy13_17_hhs_vaccines_marked <- fy13_17_hhs_6505 %>% 
  mutate(vaccine = ifelse(grepl("VACCINE", description_of_requirement)==TRUE, 1, 0 ) )
  
  fy13_17_hhs_vaccines_factors <- fy13_17_hhs_vaccines_marked %>% 
    select(naics_description, funding_agency_name, vendor_name, vaccine) %>% 
    mutate_all(as.factor) %>% 
    na.omit()
  
  svmfit <- svm(vaccine ~ naics_description+vendor_name+funding_agency_name, fy13_17_hhs_vaccines_factors)
  svm_pred <- predict(svmfit, fy17hhs_vaccine_factors)
}


svmfit <- 

prep_vaccine_data_prep <- function()
{
  #subset the source dataset appropriately
  fy16_hhs_pharma_collected <- fy16testing_transactions %>% filter(product_or_service_code == "6505") %>% 
    filter(funding_department_name == "HEALTH AND HUMAN SERVICES, DEPARTMENT OF") %>%
    collect()
  #create label column
  fy16_hhs_vaccines_marked <- fy16_hhs_pharma_collected %>% 
    mutate(vaccine = ifelse(grepl("VACCINE", description_of_requirement)==TRUE, 1, 0 ) )
  
  #create balanced training data set
  fy16_hhs_vaccines_factors <- fy16_hhs_vaccines_marked %>% 
    select(naics_description, funding_agency_name, vendor_name, vaccine) %>% 
    mutate_all(as.factor) %>% 
    na.omit()
  
  
 fy17_hhs_pharma_collected <- fy17testing_transactions %>% filter(product_or_service_code == "6505") %>% 
   filter(funding_department_name == "HEALTH AND HUMAN SERVICES, DEPARTMENT OF") %>%
   collect()
 #create label column
# fy17_hhs_vaccines_marked <- fy17_hhs_pharma_collected %>% 
#   mutate(vaccine = ifelse(grepl("VACCINE", description_of_requirement)==TRUE, 1, 0 ) )
 fy17hhs_vaccine_factors <-  fy17_hhs_pharma_collected %>% 
   select(naics_description, funding_agency_name, vendor_name)%>%
   na.omit()
 
 #create balanced training data set
 fy17hhs_vaccine_factors <-  fy17hhs_vaccine_factors %>% mutate_all(as.factor) %>% na.omit()
 
 knnfit <- knncat(fy16hhs_vaccine_factors, classcol = 4)  
 knnpred <- predict(object = knnfit, train = fy16hhs_vaccine_factors, newdata = fy17hhs_vaccine_factors, train.classcol = 4)
}