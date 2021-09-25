if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("D:/Documents/Uni/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

library(glue)
library(fs)
library(tidyverse)
library(magrittr)
library(lubridate)
library(disk.frame)
library(textclean)
library(quanteda)
library(reticulate)

source("./resources/disk.frame_extensions.R")

################################################################################

workers <- 5

setup_disk.frame(workers=workers)
quanteda_options(threads=workers)

################################################################################

data_dir <- path_wd("data")
temp_dir <- dir_create(path(data_dir, "tmp"), recurse=FALSE)

unixtools::set.tempdir(as(path_real(temp_dir), "character"))

if(!"vader" %in% reticulate::conda_list(conda="~/anaconda3/bin/conda")$name){
  reticulate::conda_create("vader", conda="~/anaconda3/bin/conda")
  reticulate::conda_install("vader", "vaderSentiment", pip=TRUE,
                            conda="~/anaconda3/bin/conda")
}

################################################################################

abbrev_regex <- 
  readxl::read_xlsx("./resources/abbrev_adj.xlsx") %>% 
  arrange(-nchar(abbrev)) %>% 
  transmute(pattern = 
              str_c("(?i)(?<=(^| ))", str_replace_all(abbrev, "\\.", "\\\\\\."),
                    "(?=( |$))"),
            replacement = str_remove_all(abbrev, fixed("."))) %>% 
  deframe()

################################################################################
################################################################################
################################################################################

convers_full_sentence_pol_score <-
  disk.frame(path(data_dir, "02-convers_full_clean.df")) %>% 
  reset_workers_df() %>% 
  resuming_cmap(function(.chunk){

    reticulate::use_condaenv("vader", conda="~/anaconda3/bin/conda", 
                             required=TRUE)
    vader <- reticulate::import("vaderSentiment.vaderSentiment")
    vader_pol_scores <- vader$SentimentIntensityAnalyzer()$polarity_scores
    
    
    .chunk_mod <- 
      .chunk %>% 
      dplyr::mutate(
        sentences = 
          text %>% 
          # HANDLE ABBREVIATIONS, DOTTED ACRONYMS AND NUMBER SEPARATORS
          stringr::str_replace_all(abbrev_regex) %>% 
          stringr::str_remove_all("(?<=\\b[[:alnum:]])\\.") %>%
          stringr::str_remove_all("(?<=[[:digit:]])[.,]+(?=[[:digit:]])") %>%
          # ...
          stringr::str_squish() %>% 
          stringr::str_split("(?<=([.!?])(?![.!?]))") %>% 
          purrr::map(stringr::str_subset, "[^[^[:graph:]][:punct:]~]") %>% 
          purrr::map(stringr::str_squish)
      ) %>% 
      dplyr::select(convers_id, status_id, sentences) %>% 
      tidyr::unnest_longer(sentences, values_to="sentence_text", 
                           indices_to="sentence_id") %>% 
      dplyr::mutate(sentence_count_word = 
                      stringi::stri_count_words(sentence_text)) %>% 
      dplyr::mutate(sentence_count_emoji = 
                      emo::ji_count(sentence_text)) %>% 
      dplyr::mutate(sentence_pol_score = 
                      purrr::map(sentence_text, vader_pol_scores)) %>% 
      tidyr::unnest_wider(sentence_pol_score, names_sep="_") %>% 
      dplyr::rename(sentence_pol_score_com = sentence_pol_score_compound)
      
    return(.chunk_mod)
    
  }) %>% 
  reset_workers_df() %>%
  copy_df_to(outdir=path(data_dir, "04-convers_full_sentence_pol_score.df"))

################################################################################

convers_full_status_pol_score <- 
  disk.frame(path(data_dir, "04-convers_full_sentence_pol_score.df")) %>%
  reset_workers_df() %>%
  resuming_cmap(function(.chunk){

    str_replace <- stringr::str_replace

    .chunk_mod <-
      .chunk %>%
      select(matches("(_id$|^sentence_(pol|count)_)")) %>%
      group_by(convers_id, status_id) %>%
      summarise(
        across(matches("^sentence_pol_score_"), mean,
               .names="{str_replace(.col, 'sentence', 'status')}_mean"),
        across(matches("^sentence_pol_score_"), weighted.mean,
               w=sentence_count_word + sentence_count_emoji,
               .names="{str_replace(.col, 'sentence', 'status')}_wean"),
        across(matches("^sentence_count_"), sum,
               .names="{str_replace(.col, 'sentence', 'status')}"),
        status_count_sentence = 
          n_distinct(sentence_id),
        status_pol_score_com_extreme =
          sentence_pol_score_com %>%
          pluck(which.max(abs(.))),
        .groups="drop"
      )
    
    return(.chunk_mod)

  }) %>%
  reset_workers_df(.change_workers=workers) %>%
  copy_df_to(path(data_dir, "04-convers_full_status_pol_score.df"))
