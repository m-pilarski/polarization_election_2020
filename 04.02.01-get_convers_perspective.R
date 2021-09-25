if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("I:/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

################################################################################

# Model Cards
# https://developers.perspectiveapi.com/s/about-the-api-model-cards
# Attributes & Languages
# https://developers.perspectiveapi.com/s/about-the-api-attributes-and-languages
# What do Perspective’s scores mean?
# https://medium.com/jigsaw/what-do-perspectives-scores-mean-113b37788a5d

################################################################################

devtools::install_github("favstats/peRspective", quiet=TRUE)

library(fs)
library(bit64)
library(disk.frame)f
library(tidyverse)
library(peRspective)

source("./resources/disk.frame_extensions.R")
source("./resources/ggplot_theme_set.R")

################################################################################

data_dir <- path_wd("data")
temp_dir <- dir_create(path(data_dir, "tmp"), recurse=FALSE)
prsp_api_key <- "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

################################################################################

workers <- 5

options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=workers)
quanteda::quanteda_options(threads=workers)
fst::threads_fst(1L)

################################################################################

prsp_attr_nytimes <- 
  c("attack_on_author", "attack_on_commenter", "incoherent", "inflammatory", 
    "likely_to_reject", "obscene", "spam", "unsubstantial")

################################################################################

convers_stm_vocab <- 
  chuck(read_rds(path(data_dir, "02-convers_stm_data.rds")), "vocab")

# convers_info_prsp <- read_rds(path(data_dir, "04-convers_info_prsp.rds"))
convers_info_prsp <-
  read_rds(path(data_dir, "02-convers_info.rds")) %>%
  transmute(status_text = text, status_id = as.character(convers_id)) %>%
  prsp_stream(text=status_text,
              text_id=status_id,
              languages="en",
              score_model=peRspective::prsp_models,
              verbose=TRUE,
              safe_output=TRUE,
              key=prsp_api_key) %>%
  rename_with(str_to_lower) %>%
  rename(convers_id = text_id) %>%
  mutate(convers_id = as.integer64(convers_id)) %>%
  write_rds(path(data_dir, "04-convers_info_prsp.rds"))

convers_info_prsp %>% 
  filter(toxicity > 0.75) %>% 
  inner_join(read_rds(path(data_dir, "02-convers_info.rds"))) %>% 
  select(convers_candidate, text, toxicity) %>% 
  view()

################################################################################

# status_id_term_count_is_vocab <-
#   read_rds(path(data_dir, "05-status_id_term_count_is_vocab.rds"))
status_id_term_count_is_vocab <-
  disk.frame(path(data_dir, "02-convers_full_term_count.df")) %>%
  reset_workers_df() %>%
  cmap_dfr(function(.chunk){

    .chunk_mod <-
      .chunk %>%
      mutate(is_vocab = term %in% convers_stm_vocab) %>%
      group_by(status_id) %>%
      summarise(term_count = sum(count),
                term_count_is_vocab = sum(count[is_vocab]),
                .groups="drop")

    return(.chunk_mod)

  }) %>%
  reset_workers_df() %>%
  write_rds(path(data_dir, "05-status_id_term_count_is_vocab.rds"))

status_created_at_date_min <-
  disk.frame(path(data_dir, "02-convers_full_raw.df")) %>%
  srckeep("created_at") %>%
  cmap_dfr(function(.chunk){
    slice_min(.chunk, created_at, with_ties=FALSE)
  }) %>%
  slice_min(created_at, with_ties=FALSE) %>%
  pull(created_at) %>%
  lubridate::date()

# convers_status_id_created_at <-
#   read_rds(path(data_dir, "05-convers_status_id_created_at.rds"))
convers_status_id_created_at <-
  disk.frame(path(data_dir, "02-convers_full_raw.df")) %>%
  reset_workers_df() %>%
  cmap_dfr(function(.chunk){

    .chunk_mod <-
      .chunk %>%
      distinct(status_id, .keep_all=TRUE) %>%
      transmute(convers_id, status_id,
                status_created_at_day =
                  created_at %>%
                  lubridate::with_tz("EST") %>%
                  lubridate::date() %>%
                  magrittr::subtract(status_created_at_date_min) %>%
                  lubridate::time_length(unit="day"))

    return(.chunk_mod)

  }) %>%
  reset_workers_df() %>%
  write_rds(path(data_dir, "05-convers_status_id_created_at.rds"))

dir_create(fs::path(data_dir, "04-convers_sample_prsp.df"))

status_id_has_prsp <- 
  disk.frame(path(data_dir, "04-convers_sample_prsp.df")) %>% 
  srckeep("status_id") %>% 
  cmap_dfr(identity)

status_id_get_prsp <-
  convers_status_id_created_at %>%
  semi_join(filter(status_id_term_count_is_vocab,
                   term_count >= 3, term_count/term_count_is_vocab >= (2/3)),
            by="status_id") %>%
  inner_join(select(read_rds(path(data_dir, "05-convers_info_extended.rds")),
                    convers_id, convers_candidate),
             by="convers_id") %>%
  select(status_id, status_created_at_day, convers_candidate) %>%
  distinct(status_id, .keep_all=TRUE) %>% 
  nest(status_id_full = c(status_id)) %>% 
  mutate(has_n_prsp = map_int(status_id_full, function(.status_id_full){
    nrow(semi_join(.status_id_full, status_id_has_prsp, by="status_id"))
  })) %>% 
  mutate(status_id_sample = map2(status_id_full, has_n_prsp, function(...){
    slice_sample(anti_join(..1, status_id_has_prsp, by="status_id"), 
                 n=max(5e3-..2, 0))
  })) %>% 
  pull(status_id_sample) %>% 
  bind_rows()

rm(convers_status_id_created_at, status_id_term_count_is_vocab); gc()
gdata::ll(unit="MB", all.names=TRUE)

convers_sample_clean <-
  disk.frame(path(data_dir, "02-convers_full_clean.df")) %>%
  reset_workers_df() %>%
  cmap_dfr(function(.chunk){
    select(inner_join(.chunk, status_id_get_prsp, by="status_id"),
           status_id, status_text = text)
  }) %>%
  reset_workers_df() %>% 
  distinct(status_id, .keep_all=TRUE) %>% 
  slice_sample(prop=1) %>% 
  as.disk.frame(outdir=tmp_df(), nchunks=ceiling(nrow(.)/1e3))

beepr::beep()
reset_workers_df(.change_workers=1)

convers_sample_prsp <- resuming_cmap(convers_sample_clean, function(.chunk){
    
  library(peRspective)
  
  .chunk_mod <- 
    .chunk %>%
    dplyr::transmute(status_text, status_id = as.character(status_id)) %>%
    peRspective::prsp_stream(text=status_text, 
                             text_id=status_id, 
                             languages="en", 
                             score_model=peRspective::prsp_models, 
                             key=prsp_api_key,
                             safe_output=TRUE) %>%
    dplyr::rename_with(stringr::str_to_lower) %>%
    dplyr::rename(status_id = text_id) %>%
    dplyr::mutate(status_id = bit64::as.integer64(status_id)) %>% 
    dplyr::filter(error == "No Error")
  
  if(nrow(.chunk_mod) > 0){
    disk.frame::add_chunk(
      disk.frame::disk.frame(fs::path(data_dir, "04-convers_sample_prsp.df")),
      data.table::as.data.table(.chunk_mod)
    )
  }
  
  rm(.chunk_mod); gc(); return(NULL)
  
}, .workers=1, .resume=FALSE)

################################################################################
################################################################################
################################################################################

