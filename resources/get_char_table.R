if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("D:/Documents/Uni/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

# library(glue)
library(tidyverse)
# library(httr)
# library(xml2)
# library(rvest)

################################################################################

char_table <-
  "https://www.unicode.org/Public/UCD/latest/ucd/UnicodeData.txt" %>% 
  read_lines() %>% 
  str_extract("^[:xdigit:]{4};[^;]+") %>% 
  discard(is.na) %>% 
  str_c(collapse="\n") %>% 
  read_delim(";", col_names=c("code", "desc")) %>% 
  mutate(desc = str_to_lower(desc)) %>% 
  write_rds("./resources/char_table.rds")

latin_letters_regex <- 
  char_table %>% 
  filter(str_detect(desc, "^latin \\w+ letter \\w")) %>% 
  mutate(case = str_replace(desc, "^latin (\\w+) letter \\w.*", "\\1"),
         letter = str_replace(desc, "^latin \\w+ letter (\\w).*", "\\1"),
         replacement = 
           case_when(case == "capital" ~ str_to_upper(letter),
                     case != "capital" ~ str_to_lower(letter))) %>% 
  group_by(replacement) %>% 
  summarise(pattern = str_c("(", str_c(str_c("\\u", code), collapse="|"), ")"),
            .groups="drop") %>% 
  write_rds("./resources/latin_letters_regex.rds")

bracket_chars <-
  "https://www.unicode.org/Public/UCD/latest/ucd/BidiBrackets.txt" %>%
  read_lines() %>%
  str_subset("^[^#]") %>%
  str_split(" *; +") %>%
  map_dfr(set_names, c("value_this", "value_other", "name")) %>%
  mutate(side = str_extract(name, "^(o|c|u)(?= # )")) %>%
  split(pull(., side)) %>%
  map(function(.subset){
    str_c("(", str_c(str_c("\\u", .subset$value_this), collapse="|"), ")")
  }) %>%
  write_rds("./resources/bracket_chars.rds")
