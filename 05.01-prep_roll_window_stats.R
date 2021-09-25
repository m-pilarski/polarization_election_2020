if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("D:/Documents/Uni/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

library(glue)
library(fs)
library(bit64)
library(tidyverse)
library(magrittr)
library(lubridate)
library(disk.frame)

source("./resources/disk.frame_extensions.R")
source("./resources/ggplot_theme_set.R")
source("./resources/plot_to_file.R")
source("./resources/bimodality_coefficient.R")
source("./resources/divisiveness_coefficient.R")

################################################################################

set_temp_dir <- function(.temp_dir=temp_dir){
  if(grepl("(?i)linux", sessionInfo()$running)){
    .dir <- fs::path(.temp_dir, digest::digest(list(Sys.getpid(), Sys.time())))
    unixtools::set.tempdir(fs::dir_create(.dir, recurse=FALSE))
  }else{
    warning("only works on linux")
  }
  return(invisible(.temp_dir))
}

################################################################################

workers <- 6
options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=1)
fst::threads_fst(nr_of_threads=workers)

################################################################################

data_dir <- path_wd("data")
temp_dir <- set_temp_dir(path(data_dir, "tmp"))

################################################################################

election_day <- ymd("2020-11-03", tz="EST")
study_tscope <- interval(election_day-weeks(10), ymd("2021-01-06", tz="EST"))

################################################################################

convers_info <- read_rds(path(data_dir, "02-convers_info.rds"))

################################################################################
################################################################################
################################################################################

chunk_left_join_df <- function(.chunk, .df_join){

  .chunk <- dplyr::semi_join(.chunk, convers_info, by="convers_id")
  
  if(nrow(.chunk) == 0){return(tibble::tibble(NULL))}
  
  .colnames_join <- 
    purrr::discard(colnames(.df_join), magrittr::is_in, colnames(.chunk)) %>% 
    purrr::prepend("status_id")

  .df_join_subset_mem <-
    .df_join %>%
    disk.frame:::select.disk.frame(tidyselect::any_of(.colnames_join)) %>%
    disk.frame:::filter.disk.frame(
      status_id %in% purrr::chuck(.chunk, "status_id")
    ) %>%
    disk.frame:::collect.disk.frame() %>% 
    distinct(status_id, .keep_all=TRUE)

  .chunk_mod <- dplyr::left_join(.chunk, .df_join_subset_mem, by="status_id")
  
  gc()
  
  return(data.table::as.data.table(.chunk_mod))
  
}

################################################################################
################################################################################
################################################################################

convers_full_status_stats_hlf <-
  disk.frame(path(data_dir, "02-convers_full_raw.df")) %>%
  resuming_cmap(function(.chunk){

    .chunk_mod <-
      .chunk %>%
      dplyr::select(status_id, convers_id, user_id,
                    status_reply_to_status_id = reply_to_status_id,
                    status_lang=lang, status_created_at=created_at) %>%
      dplyr::semi_join(convers_info, by="convers_id") %>%
      dplyr::distinct(convers_id, status_id, .keep_all=TRUE) %>%
      dplyr::arrange(convers_id, status_id)

    return(data.table::as.data.table(.chunk_mod))

  }, .resume=TRUE) %>%
  resuming_cmap(
    .f=chunk_left_join_df,
    .df_join=disk.frame(path(data_dir, "02-convers_full_net_stat.df")),
    .resume=TRUE
  ) %>%
  resuming_cmap(
    .f=chunk_left_join_df,
    .df_join=disk.frame(path(data_dir, "04-convers_full_status_pol_score.df")),
    .resume=TRUE
  ) %>%
  write_disk.frame(outdir=path(data_dir, "05-convers_full_status_stats_hlf.df"),
                   overwrite=TRUE)

disk.frame::setup_disk.frame(workers=1)
disk.frame::setup_disk.frame(workers=4); fst::threads_fst(nr_of_threads=2)

convers_full_status_stats <-
  disk.frame(path(data_dir, "05-convers_full_status_stats_hlf.df")) %>%
  resuming_cmap(
    .f=chunk_left_join_df,
    .df_join=disk.frame(path(data_dir, "03-convers_full_topic_group.df")),
    .resume=TRUE
  ) %>%
  resuming_cmap(
    .f=chunk_left_join_df,
    .df_join=disk.frame(path(data_dir, "04-convers_sample_prsp.df")),
    .resume=TRUE
  ) %>%
  write_disk.frame(outdir=path(data_dir, "05-convers_full_status_stats.df"),
                   overwrite=TRUE)

################################################################################
################################################################################
################################################################################

status_id_has_term_count_and_vocab <- 
  read_rds(path(data_dir, "02-status_id_has_term_count_and_vocab.rds"))

################################################################################

roll_window_stats <-
  expand_grid(
    convers_candidate = 
      unique(pull(convers_info, convers_candidate)),
    status_topic_group = 
      unique(pull(read_rds("./data/03-stm_topic_stats.rds"), topic_group)),
    date_end = 
      seq(int_start(study_tscope), int_end(study_tscope), "days") %>% 
      date() %>% 
      `tz<-`("EST")
  ) %>%
  arrange(status_topic_group=="anderes Thema",
          convers_candidate=="Mike_Pence") %>% 
  as.disk.frame(nchunks=nrow(.), outdir=tmp_df()) %>% 
  resuming_cmap(function(.chunk){
    
    fst::threads_fst(nr_of_threads=2)
    
    .pars_grid <- expand_grid(
      window_days = c(1, 3, 7),
      max_dist_from_start = c(1, Inf)
    )
    
    .convers_id_vec <- 
      convers_info %>% 
      filter(convers_candidate == .chunk$convers_candidate) %>% 
      chuck("convers_id")
    
    .chunk_stats <-
      disk.frame(path(data_dir, "05-convers_full_status_stats.df")) %>% 
      cmap_dfr(function(..chunk){
        
        ..chunk %>%
          mutate(
            status_topic_group =
              case_when(status_topic_group_prop < 0.5 ~ "anderes Thema",
                        TRUE ~ as.character(status_topic_group)) %>%
              factor(levels=levels(status_topic_group))
          ) %>%
          filter(
            is.finite(status_dist_from_convers_start),
            `%in%`(convers_id, .convers_id_vec),
            `==`(status_topic_group, .chunk$status_topic_group),
            `<`(status_created_at, .chunk$date_end + days(1)),
            `>=`(status_created_at,
                 .chunk$date_end - days(max(.pars_grid$window_days) - 1)),
          )
        
      }) %>%
      semi_join(
        status_id_has_term_count_and_vocab, 
        by="status_id"
      ) %>%
      mutate(
        
        status_pol_score_emo_mean = 1 - status_pol_score_neu_mean,
        status_pol_score_com_mean_cut_2 =
          status_pol_score_com_mean %>%
          abs() %>%
          cut.default(c(0,0.05,1), include.lowest=TRUE, right=TRUE) %>%
          as.numeric() %>%
          `-`(1) %>%
          `/`(max(., na.rm=TRUE)) %>%
          `*`(sign(status_pol_score_com_mean)),
        status_pol_score_com_mean_cut_3 =
          status_pol_score_com_mean %>%
          abs() %>%
          cut.default(c(0,0.05,0.5,1), include.lowest=TRUE, right=TRUE) %>%
          as.numeric() %>%
          `-`(1) %>%
          `/`(max(., na.rm=TRUE)) %>%
          `*`(sign(status_pol_score_com_mean))
        
      )
    
    .window_summary <-
      .pars_grid %>%
      group_by(across(everything())) %>%
      group_modify(function(..gdata, ..gkeys){
        
        ..gsummary <-
          .chunk_stats %>%
          filter(
            `>=`(status_created_at,
                 .chunk$date_end - days(..gkeys$window_days - 1)),
            `<=`(status_dist_from_convers_start,
                 ..gkeys$max_dist_from_start)
          ) %>%
          summarise(
            
            status_count = n_distinct(status_id),
            across(matches("^status_pol_score_com_mean", perl=TRUE),
                   list(var=var, mean=mean, sd=sd,
                        bimod_mouse = bimodality_coefficient,
                        bimod_modes = modes::bimodality_coefficient,
                        divi = divisiveness_coefficient),
                   na.rm=TRUE, .names="{str_remove(.col, '^status_')}_{.fn}"),
            across(matches("^status_pol_score_com_mean_cut", perl=TRUE),
                   function(..pol_com_mean_cut){
                     ..pol_com_mean_cut %>%
                       table() %>%
                       enframe(name="value", value="count") %>%
                       mutate(across(count, as.integer)) %>%
                       jsonlite::toJSON()
                   }, .names="{str_remove(.col, '^status_')}_json_table"),
            status_prsp_count = sum(!is.na(error)),
            across(toxicity:unsubstantial,
                   function(..prsp_score){
                     `/`(sum(..prsp_score > 0.75, na.rm=TRUE),
                         sum(!is.na(..prsp_score), na.rm=TRUE))
                   }, .names="prsp_{.col}_share"),
            prsp_attack_on_someone_share =
              mean(attack_on_author > 0.75 | attack_on_commenter > 0.75,
                   na.rm=TRUE),
            indirect_share =
              mean(status_dist_from_convers_start > 1),
            indirect_height_mean =
              mean(discard(status_dist_from_convers_start, `==`, 1))
            
          ) %>%
          rename_with(function(..name){str_c("window_", ..name)},
                      -starts_with("window_"))
        
        return(..gsummary)
        
      }) %>%
      ungroup()
    
    .chunk_mod <- bind_cols(.chunk, .window_summary)
    
    gc()
    
    return(.chunk_mod)
    
  }, .workers=4, .scheduling=Inf) %>%
  cmap_dfr(identity) %>%
  write_rds(path(data_dir, "05-roll_window_stats.rds"))

################################################################################
################################################################################
################################################################################