# Aim -  to download a series of zipped files from cdc wonder using scripts alone 

pacman::p_load(
  tidyverse, 
  here,
  curl,
  stringr
)


# What are the uris? 


# ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2018us.zip
# ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2017us.zip
# ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2016us.zip

sources <- c(
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2018us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2017us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2016us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2015us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2014us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2013us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2012us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2011us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2010us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2009us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2008us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2007us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2006us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2005us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2004us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2003us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2002us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2001us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort2000us.zip",
  "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/mortality/mort1999us.zip"

  
)

tbl <- tibble(
  source_year = 2018:1999,
  source = sources
)


# For each of these want to download a file as follows:

source_and_tidy <- function(url){
  temp  <- tempfile() 
  temp2 <- tempfile() 
  

  #dir.create("big_data")
  
  # This downloads the zip file to a temp file object called temp
  curl_download(url = url, destfile = temp, quiet = FALSE, mode = "wb")
  
  # This unzips the contents of temp to a temporary file directory called temp2
  
  unzip(zipfile = temp, exdir = temp2)
  
  # The object can then be accessed by combining the location of temp2 with its object inside as follows
  readLines(file.path(temp2, dir(temp2)[1]), n = 10)
  
  # And the location of the variables is described here: 
  # https://www.cdc.gov/nchs/data/dvs/Multiple_Cause_Record_Layout_2018-508.pdf
  
  # So I want the following (variable name, tape locations)
  # Data year : 102-105
  # Sex : 69
  # Race : 445-450
  # Age : 70-82
  # Hispanic Origin : 484-488
  # ICD-10 Code : 146-149
  # 39 cause recode : 160-161
  # 130 cause recode : 157-159
  df <- 
    read_fwf(
      file = file.path(temp2, dir(temp2)[1]), 
      fwf_cols(
        year = c(102, 105),
        sex = c(69, 69),
        race = c(445,446),
        age = c(71, 73),
        hisp = c(488, 488),
        icd10 = c(146,149)
      ), 
      col_types = cols(
        year = col_number(), sex = col_character(), race = col_character(),
        age = col_number(),
        hisp = col_character(), icd10 = col_character()
      )
    )
  
  df <- 
    df %>% 
    filter(age != 999) %>%
    mutate(age = age - 1) %>% 
    mutate(
      sex = case_when(
        sex == "M" ~ "male",
        sex == "F" ~ "female",
        TRUE       ~ NA_character_
      ),
      race = case_when(
        race == "01"   ~ "white", 
        race == "02"   ~ "black",
        TRUE          ~ "other"
      ),
      hisp = case_when(
        hisp == "6"     ~ "Non-hispanic White",
        hisp == "7"     ~ "Non-hispanic Black",
        hisp == "8"     ~ "Non-hispanic Other",
        hisp == "9"     ~ NA_character_,
        TRUE            ~ "Hispanic"
      ),
      icd10_dotted = case_when(
        str_detect(str_sub(icd10, 4), "[0-9]") ~ str_c(str_sub(icd10, 1,3), ".", str_sub(icd10,4)),
        TRUE ~ icd10
      )
    ) %>% 
    group_by(year, age, sex, hisp, icd10_dotted) %>% 
    tally() 
  
  df
}

deaths_df <- tbl %>% 
  mutate(data = map(source, source_and_tidy))

# this works for years between 2003 and 2018
deaths_df <- 
  deaths_df %>% 
    filter(between(source_year, 2003, 2018)) %>% 
    unnest(data) %>% 
    select(-source_year, -source)


#Legal intervention is Y35
# https://icd.codes/icd10cm/Y35

# Assault/homicide is X85-Y09

# What's the total number of deaths by race, hisp, sex, year, and age? 





# USer guides:
# https://www.cdc.gov/nchs/nvss/mortality_public_use_data.htm


# So the process should be: 

# Download the zip file
# unzip
# read in as fwf
# Aggregate to counts of interest
# Save only this
# Move onto next 

# This is conditional on the fwf being consistent (or consistent enough) across years

# 
# https://readr.tidyverse.org/reference/read_fwf.html


# Population sizes:

# https://wonder.cdc.gov/Bridged-Race-v2018.HTML

# This isn't sufficient as doesn't have race as well as hispanic/non-hispanic. Extracting again
# Done
 
 
# Have downloaded to unzipped_data/cdc_2018/Bridged-Race Population Estimates 1990-2018.txt

pop_sizes <- read_delim("unzipped_data/cdc_2018/Bridged-Race Population Estimates 1990-2018.txt", 
                        delim = "\t")
tidied_pop_sizes <- 
  pop_sizes %>% 
    mutate(
      ethnic_fourgroup = case_when(
        Ethnicity == "Hispanic or Latino" ~ "Hispanic",
        Race == "White"                   ~ "White Non-Hispanic",
        Race == "Black or African American" ~ "Black Non-Hispanic",
        Race %in% c("American Indian or Alaska Native", "Asian or Pacific Islander") ~ "Other Non-Hispanic",
        TRUE ~ NA_character_
      )
    ) %>% 
    filter(Gender %in% c("Male", "Female")) %>% 
    select(
      age = `Age Code`, year = `Yearly July 1st Estimates`, 
      ethnicity = ethnic_fourgroup, sex = Gender,
      N = Population
      ) %>% 
    filter(complete.cases(.)) %>% 
    mutate(sex = tolower(sex)) %>% 
    group_by(age, year, ethnicity, sex) %>%  
    summarise(N = sum(N)) %>% 
    ungroup() 

rm(pop_sizes)


### Death Rates 


# Deaths through legal intervention 


n_deaths_legal_intervention <-    
  expand_grid(
      age = 0:115, 
      year = 2003:2018, 
      ethnicity = c(
        "Other Non-Hispanic", "White Non-Hispanic", "Black Non-Hispanic", "Hispanic"
        ),
      sex = c("male", "female")
      ) %>%
    left_join(
      deaths_df %>% 
        filter(str_detect(icd10_dotted, "^Y35")) %>% # Legal intervention
         mutate(ethnicity = case_when(
           hisp == "Hispanic" ~ "Hispanic",
           hisp == "Non-hispanic Black" ~ "Black Non-Hispanic",
           hisp == "Non-hispanic White" ~ "White Non-Hispanic",
           hisp == "Non-hispanic Other" ~ "Other Non-Hispanic"
         )) %>% 
         group_by(age, year, sex, ethnicity)  %>% 
         summarise(n_deaths = sum(n)) %>% 
         ungroup()
    ) %>% 
    mutate(n_deaths = ifelse(is.na(n_deaths), 0, n_deaths))

# Homicide 
n_deaths_assault_homicide <-    
  expand_grid(
    age = 0:115, 
    year = 2003:2018, 
    ethnicity = c(
      "Other Non-Hispanic", "White Non-Hispanic", "Black Non-Hispanic", "Hispanic"
    ),
    sex = c("male", "female")
  ) %>%
  left_join(
    deaths_df %>% 
      filter(str_detect(icd10_dotted, 
                        glue::glue_collapse(glue::glue(
                          glue::glue_collapse(glue::glue("X{85:99}"), sep = "|"), 
                          glue::glue_collapse(glue::glue("^Y0{0:9}"), sep = "|")
                        ), sep = "|")
        )
      ) %>% # Assault/Homicide
      mutate(ethnicity = case_when(
        hisp == "Hispanic" ~ "Hispanic",
        hisp == "Non-hispanic Black" ~ "Black Non-Hispanic",
        hisp == "Non-hispanic White" ~ "White Non-Hispanic",
        hisp == "Non-hispanic Other" ~ "Other Non-Hispanic"
      )) %>% 
      group_by(age, year, sex, ethnicity)  %>% 
      summarise(n_deaths = sum(n)) %>% 
      ungroup()
  ) %>% 
  mutate(n_deaths = ifelse(is.na(n_deaths), 0, n_deaths))



n_deaths_assault_homicide %>% 
  ggplot(aes(x = year, y = age, fill= n_deaths)) + 
  facet_grid(ethnicity ~ sex) + 
  geom_tile() 

# Now add N

death_rates_assault_homicide <- 
  n_deaths_assault_homicide %>% 
    left_join(tidied_pop_sizes)


# Population standardisation 
# https://seer.cancer.gov/stdpopulations/
  
standard_popn <- 
  read_fwf(
    file = "https://seer.cancer.gov/stdpopulations/stdpop.singleagesthru99.txt", 
    fwf_cols(
      year = c(1, 4),
      age = c(5, 6),
      std_pop = c(7,14)
    ),
    col_types = cols(
      year = col_number(), age = col_number(), std_pop = col_number()
    )
  ) %>% 
  filter(year == 120) %>% 
  select(-year)


# Merge this (truncate to 99+)

death_rates_legal_intervention %>% 
  mutate(age = ifelse(age > 99, 99, age)) %>% 
  group_by(age, year, ethnicity, sex) %>% 
  summarise(n_deaths = sum(n_deaths), N = sum(N)) %>% 
  ungroup() %>% 
  left_join(standard_popn) %>% 
  mutate(N = ifelse(is.na(N), 0, N)) %>% 
  mutate(mr = (n_deaths + 0.0005) / (N + 0.0005)) %>% 
  group_by(year, ethnicity, sex) %>% 
  mutate(tmp = mr * std_pop / sum(std_pop)) %>% 
  ungroup() %>%
  group_by(year, ethnicity, sex) %>% 
  summarise(std_mr = sum(tmp)) %>% 
  ungroup() %>% 
  ggplot(aes(x = year, y = std_mr, group = ethnicity, colour = ethnicity)) + 
  geom_line() +
  facet_wrap(~sex) 


# How about numbers between ages 15 and 40? 
death_rates_legal_intervention %>%
  filter(between(age, 15, 45)) %>% 
  group_by(year, ethnicity, sex) %>% 
  summarise(
    n = sum(n_deaths), 
    N = sum(N)
  ) %>% 
  ungroup() %>% 
  mutate(mr = 10^6 * n / N) %>% 
  ggplot(aes(x = year, y = mr, colour = ethnicity)) + 
  geom_line() + 
  facet_wrap(~sex)


  mutate(age = ifelse(age > 99, 99, age)) %>% 
  group_by(age, year, ethnicity, sex) %>% 
  summarise(n_deaths = sum(n_deaths), N = sum(N)) %>% 
  ungroup()


death_rates_legal_intervention %>% 
  mutate(age = ifelse(age > 99, 99, age)) %>%
  group_by(age, year, ethnicity, sex) %>% 
  summarise(n_deaths = sum(n_deaths), N = sum(N)) %>% 
  ungroup() %>% 
  left_join(standard_popn) %>% 
  mutate(N = ifelse(is.na(N), 0, N)) %>% 
  mutate(age_group = cut(age, breaks = c(0, seq(5, 90, by = 5)), include.lowest = TRUE)) %>%
  mutate(!is.na(age_group)) %>% 
  group_by(age_group, year, ethnicity, sex) %>% 
  summarise(n_deaths = sum(n_deaths), N = sum(N), std_pop = sum(std_pop)) %>% 
  ungroup() %>% 
  mutate(mr = n_deaths / N) %>% 
  group_by(year, ethnicity, sex) %>% 
  mutate(share_pop = std_pop / sum(std_pop)) %>% 
  mutate(tmp = mr * share_pop) %>% View() 
  summarise(smr = sum(tmp)) %>% 
  ungroup() 












