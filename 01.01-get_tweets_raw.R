if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("F:/Documents/uni/masterarbeit/analysis/")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis/")
}

options(future.globals.maxSize=Inf)

library(fs)
library(glue)
library(tidyverse)
library(magrittr)
library(lubridate)
library(rtweet)
library(bit64)
library(disk.frame)

source("./resources/best_map.R")
source("./resources/best_token.R")
source("./resources/disk.frame_extensions.R")
source("./resources/ggplot_theme_set.R")
source("./resources/plot_to_file.R")

has_name <- rlang::has_name
extract <- magrittr::extract

################################################################################

token_env <- rlang::env()
token_list <- read_rds("./resources/token_list.rds") # list of rtweet-tokens

################################################################################

tweet_info_env <- rlang::env(
  object_size_max_B = 0L, 
  tweets_count_stored = 0L, 
  tweets_count_collected_this = 0L
)

################################################################################
################################################################################
################################################################################

write_rds(tibble(
  screen_name = c("JoeBiden", "KamalaHarris", "realDonaldTrump", "Mike_Pence"),
  tweets = list(NULL)
), glue("{data_dir}/0000-00-00.rds"))

################################################################################

start_date <- today(tzone="UTC")

data_dir <- glue("{getwd()}/data/01-tweets_raw")
temp_dir <- glue("{data_dir}/tmp")

vers_dir_ambigous <- TRUE
while(vers_dir_ambigous){
  
  tweets_path_last <- 
    last(dir_ls(data_dir, type="file", regexp="/\\d{4}\\.rds$"))
  
  vers_num_last_int <- 
    path_file(tweets_path_last) %>% 
    str_extract("^\\d{4}(?=\\.rds)") %>% 
    as.integer() %>% 
    max() 
  
  vers_dir_orphan <- 
    dir_ls(data_dir, type="directory", regexp="/\\d{4}$") %>% 
    as_tibble_col("path") %>% 
    mutate(num_int = as.integer(str_extract(path, "(?<=/)\\d{4}(?=/?)$"))) %>% 
    filter(num_int > vers_num_last_int) %>% 
    pull(path) %>% 
    str_extract("(/[^/]*){3}$")
  
  if(length(vers_dir_orphan) > 0){
    message("ORPHANED VERSION DIRS:\n", str_c(vers_dir_orphan, collapse="\n"))
    Sys.sleep(30)
  }else{
    vers_dir_ambigous <- FALSE
  }
  
}

vers_num_this <- str_pad(vers_num_last_int + 1L, 4L, "left", "0")

vers_dir_this <- glue("{data_dir}/{vers_num_this}")
tweets_path_this <- glue("{data_dir}/{vers_num_this}.rds")

dir_create(temp_dir, recurse=FALSE)
dir_create(vers_dir_this, recurse=FALSE)

################################################################################
################################################################################
################################################################################

tmp_df <- function(mode=c("get_new", "clear_old")){
  
  if(!exists(".tmp_df_env", where=.GlobalEnv, mode="environment")){
    .tmp_df_env <<- rlang::new_environment(data=list(tmp_df_list=character(0)), 
                                           parent=.GlobalEnv)
  }
  if(mode[1] == "get_new"){
    .tmp_df_new <- glue("{temp_dir}/{stringi::stri_rand_strings(1, 50)}.df")
    .tmp_df_env$tmp_df_list <- append(.tmp_df_env$tmp_df_list, .tmp_df_new)
    return(.tmp_df_new)
  }else if(mode[1] == "clear_old"){
    for(.tmp_df in .tmp_df_env$tmp_df_list){try({dir_delete(.tmp_df)})}
    .tmp_df_env$tmp_df_list <- character(0)
    return(NULL)
  }else{
    stop("mode must be in c(\"get_new\", \"clear_old\")")
  }
  
}

################################################################################
################################################################################
################################################################################

search_tweets_enhanced <- function(..., tweets_n_max=Inf, max_id=NULL){
  
  if(!rlang::env_has(token_env, "token")){
    token_env[["token"]] <- best_token(.query="search/tweets")
  }
  
  .tweets <- disk.frame(path=tmp_df())
  .tweets_list <- list()
  .tweets_n <- 0L
  .max_id <- max_id
  .resume <- TRUE
  
  while(.resume){
    
    .tweets_i <- NULL
    .tweets_i_try_count <- 1
    
    while(is_null(.tweets_i)){
      tryCatch({
        .tweets_i <- 
          search_tweets(..., n=100, max_id=.max_id, token=token_env[["token"]])
        if(is.null(.tweets_i)){stop("search returned NULL")}
      }, error=function(.error){
        .error_is_connection <- 
          as.character(.error) %>% 
          str_detect("(?i)(TIMEOUT WAS REACHED)|(COULD NOT RESOLVE HOST)")
        if(.error_is_connection){
          warning(Sys.time(), " - cannot connect", call.=FALSE)
          .tweets_i <<- NULL
          Sys.sleep(30)
        }else if(.tweets_i_try_count <= 5){
          warning("retrying after error: " , .error)
          .tweets_i <<- NULL
          .tweets_i_try_count <<- .tweets_i_try_count + 1
          Sys.sleep(10)
        }else{
          stop(.error)
        }
      }, warning = function(.warning){
        .warning_is_ratelimit <- 
          as.character(.warning) %>% 
          str_detect("(?i)RATE[- ]?LIMIT")
        if(.warning_is_ratelimit){
          token_env[["token"]] <- best_token(.query="search/tweets")
          .tweets_i <<- NULL
        }
      })
    }
    
    .tweets_list <- append(.tweets_list, list(.tweets_i))
    
    if(has_name(.tweets_i, "status_id")){
      .max_id <- 
        .tweets_i %>% 
        pluck("status_id") %>% 
        as.integer64.character() %>% 
        min() %>% 
        subtract(1) %>% 
        as.character.integer64()
    }
    
    .tweets_n <- .tweets_n + nrow(.tweets_i)
    
    .resume <- all(.tweets_n < tweets_n_max, nrow(.tweets_i) > 0)
    .append <- all(any(length(.tweets_list) == 1e3, !.resume),
                   nrow(first(.tweets_list)) > 0)
    
    if(.append){
      
      .tweets <- 
        .tweets %>% 
        add_chunk(.tweets_list %>% 
                    bind_rows() %>% 
                    select_if(~all(any(!is.na(.x)), !is.list(.x))))
      
      message(
        "####\n", 
        "  search_tweets(\"", ..1, "\")\n",
        "  nrow(tweet_this)           > " , .tweets_n, "\n",
        "  min(tweet_this$created_at) > ", min(pull(.tweets, created_at)),
        "\n####"
      )
      
      .tweets_list <- list()
      
    }
    
  }
  
  return(.tweets)
  
}

################################################################################

get_timeline_enhanced <- function(..., .tweets_n_max=3200, .max_id=NULL){
  
  if(!rlang::env_has(token_env, "user_timeline")){
    token_env[["user_timeline"]] <- best_token("statuses/user_timeline")
  }
  
  .dots <- list(...)
  .tweets <- disk.frame(path=tmp_df())
  .tweets_list <- list()
  .tweets_n_max <- min(.tweets_n_max, 3200)
  .tweets_n <- 0L
  .resume <- TRUE
  .tweets_since_id <- 
    as.integer64(if_else(is.null(.dots$since_id), "0", .dots$since_id))
  .dots <- .dots[!names(.dots) %in% c("since_id")]
  
  while(.resume){
    
    .tweets_i <- NULL
    .tweets_i_try_count <- 1
    
    while(is_null(.tweets_i)){
      tryCatch({
        .tweets_i <- 
          exec(get_timeline, !!!.dots, n=100, include_rts=TRUE, 
               exclude_replies=FALSE, home=FALSE, check=TRUE, max_id=.max_id, 
               token=token_env[["user_timeline"]])
        if(identical(dim(.tweets_i), c(0L, 0L))){
          Sys.sleep(10); warning("rate limit")
        }
        if(is.null(.tweets_i)){stop("search returned NULL")}
      }, error=function(.error){
        .error_is_connection <- 
          as.character(.error) %>% 
          str_detect("(?i)(timeout was reached)|(could not resolve host)")
        if(.error_is_connection){
          warning(Sys.time(), " - cannot connect", call.=FALSE)
          .tweets_i <<- NULL
          Sys.sleep(30)
        }else if(.tweets_i_try_count <= 5){
          warning("retrying after error: " , .error)
          .tweets_i <<- NULL
          .tweets_i_try_count <<- .tweets_i_try_count + 1
          Sys.sleep(10)
        }else{
          stop(.error)
        }
      }, warning = function(.warning){
        .warning_is_ratelimit <- 
          as.character(.warning) %>% 
          str_detect("(?i)RATE[- ]?LIMIT")
        if(.warning_is_ratelimit){
          token_env[["user_timeline"]] <- best_token("statuses/user_timeline")
          .tweets_i <<- NULL
        }
      })
    }
    
    .tweets_list <- append(.tweets_list, list(.tweets_i))
    
    if(has_name(.tweets_i, "status_id")){
      .max_id <- 
        .tweets_i %>% 
        pluck("status_id") %>% 
        as.integer64.character() %>% 
        min() %>% 
        subtract(1) %>% 
        as.character.integer64()
    }
    
    .tweets_n <- .tweets_n + nrow(.tweets_i)
    
    .resume <- !any(.tweets_n > .tweets_n_max, 
                    .tweets_since_id > min(as.integer64(.tweets_i$status_id)))
    
    if(!.resume){
      
      .tweets_tmp_tbl <- 
        .tweets_list %>% 
        bind_rows() %>% 
        select_if(~all(any(!is.na(.x)), !is.list(.x)))
      
      if("status_id" %in% names(.tweets_tmp_tbl)){
        
        .tweets_tmp_tbl <- 
          .tweets_tmp_tbl %>% 
          filter(as.integer64(status_id) > .tweets_since_id)
        
      }
      
      if(nrow(.tweets_tmp_tbl) > 0){
        
        .tweets <- add_chunk(.tweets, .tweets_tmp_tbl)
        
        message(
          "####\n", 
          "  get_timeline(", .dots$user,")\n",
          "  nrow(tweet_this)           > ", nrow(.tweets), "\n",
          "  min(tweet_this$created_at) > ", min(pull(.tweets, created_at)),
          "\n####"
        )
        
      }
      
    }
    
  }
  
  return(.tweets)
  
}

################################################################################
################################################################################
################################################################################

get_tweets <- function(.transp_r, .workers=1){ 
  
  tryCatch({
    
    stopifnot(.transp_r$screen_name != "realDonaldTrump")
    
    attr(.transp_r$tweets$from_data, "path") %<>% 
      str_replace("^.+/data/01-tweets_raw(?=/)", data_dir) %>% 
      path_expand()
    attr(.transp_r$tweets$repl_data, "path") %<>% 
      str_replace("^.+/data/01-tweets_raw(?=/)", data_dir) %>% 
      path_expand()
    
    setup_disk.frame(workers=.workers)
    
    .screen_name_dir <- glue("{vers_dir_this}/{.transp_r$screen_name}")
    dir.create(.screen_name_dir, showWarnings=FALSE)
    
    
    ############################
    # GET TWEETS FROM DELEGATE #
    ############################
    
    .from_since_id_this <-
      pluck(.transp_r, "tweets", "from_data") %>%
      collect_cols.df(.cols_want="status_id", .trans_fun=as.integer64) %>%
      drop_na(status_id) %>%
      slice_max(status_id, n=1, with_ties=FALSE) %>%
      pluck("status_id", 1) %>%
      as.character()
    
    if(.transp_r$screen_name != "realDonaldTrump"){
      
      .from_data_this <-
        get_timeline_enhanced(user=.transp_r$screen_name,
                              since_id=.from_since_id_this)
      
    }else{
      
      .from_data_this <- tibble()
      
    }
    
    
    if(nrow(.from_data_this) > 0){
      
      .from_max_id <-
        .from_data_this %>% 
        collect_cols.df(.cols_want="status_id", .trans_fun=as.integer64) %>% 
        drop_na(status_id) %>% 
        slice_max(status_id, n=1, with_ties=FALSE) %>% 
        pluck("status_id", 1) %>% 
        as.character()
      
      .from_data_last <-
        pluck(.transp_r, "tweets", "from_data",
              .default=as.disk.frame(data.frame()))
      
      .from_data <-
        list(.from_data_this, .from_data_last) %>%
        rbindlist.disk.frame(outdir=glue("{.screen_name_dir}/from.df"),
                             compress=50, overwrite=TRUE, .progress=FALSE)
      
      invisible(cmap(.from_data, function(..chunk){
        invisible(..chunk); rm(..chunk); gc(); return(NULL)
      }, lazy=FALSE))
      
    }else{
      
      .from_max_id <- .from_since_id_this
      
      .from_data <- 
        pluck(.transp_r, "tweets", "from_data") %>% 
        copy_df_to(outdir=glue("{.screen_name_dir}/from.df"))
      
    }
    
    gc()
    
    #############################################
    # GET TWEETS REPLYING TO TWEETS BY DELEGATE #
    #############################################
    
    .repl_since_id_this <-
      pluck(.transp_r, "tweets", "repl_data") %>%
      collect_cols.df(.cols_want="status_id", .trans_fun=as.integer64) %>%
      drop_na(status_id) %>%
      slice_max(status_id, n=1, with_ties=FALSE) %>%
      pluck("status_id", 1) %>%
      as.character()
    
    gc()
    
    .repl_data_this <-
      glue("@{.transp_r$screen_name} filter:replies ",
           "until:{as_date(now(tzone='UTC') - days(1))}") %>%
      search_tweets_enhanced(since_id=.repl_since_id_this, tweets_n_max=Inf,
                             verbose=FALSE)
    
    gc()
    
    if(nrow(.repl_data_this) > 0){
      
      .repl_max_id <- 
        .repl_data_this %>% 
        collect_cols.df(.cols_want="status_id", .trans_fun=as.integer64) %>% 
        drop_na(status_id) %>% 
        slice_max(status_id, n=1, with_ties=FALSE) %>% 
        pluck("status_id", 1) %>% 
        as.character()
      
      gc()
      
      .repl_data_last <- 
        pluck(.transp_r, "tweets", "repl_data", 
              .default=as.disk.frame(data.frame(status_id = character(0))))
      
      .repl_data_last_any_too_large <- 
        .repl_data_last %>% 
        attr("path") %>% 
        dir_map(fun=function(.file){
          file_size(.file) > "250MB"
        }, type="file") %>% 
        flatten_lgl() %>% 
        any()
      
      if(.repl_data_last_any_too_large){
        .repl_data_last <- rechunk_df(.repl_data_last); gc()
      }
      
      if(all(has_name(.repl_data_this, c("status_id", "reply_to_status_id")))){
        
        message("Filtering out tweets not replying to tweets by candidate")
        
        .repl_data_this_status_repl_id <-
          .repl_data_this %>% 
          collect_cols.df(.cols_want=c("status_id", "reply_to_status_id"),
                          .trans_fun=as.integer64) %>% 
          drop_na() %>% 
          distinct() 
        
        .repl_data_last_status_thread_id <-
          .repl_data_last %>%
          collect_cols.df(.cols_want=c("status_id", "thread_id"),
                          .trans_fun=as.integer64) %>% 
          drop_na() %>% 
          distinct() 
        
        gc()
        
        .repl_data_status_id <- 
          c(.repl_data_this_status_repl_id$status_id, 
            .repl_data_last_status_thread_id$status_id)
        
        .thread_status_id_this <- 
          .from_data %>% 
          delayed(function(..data){
            if(has_name(..data, "status_id")){
              ..data <- 
                ..data %>% 
                filter(!is_retweet) %>% 
                transmute(thread_id = bit64::as.integer64(status_id)) %>%
                filter(!thread_id %in% .repl_data_status_id)
            }else{
              ..data <- slice(..data, 0)
            }
            return(..data)
          }) %>%
          collect() %>% 
          as_tibble() %>% 
          drop_na() %>% 
          distinct()
        
        setup_disk.frame(workers=1)
        
        .thread_status_id_this <-
          .thread_status_id_this %>%
          mutate(status_id = best_map(thread_id, function(.thread_id){
            if(has_name(.repl_data_last, "thread_id")){
              .status_id_last <-
                .repl_data_last_status_thread_id %>%
                filter(thread_id == .thread_id) %>%
                pull(status_id)
            }else{
              .status_id_last <- character(0)
            }
            .status_id_this <- character(0)
            .status_id_this_i <- c(.thread_id, .status_id_last)
            while(length(.status_id_this_i) > 0){
              .status_id_this_i <-
                .repl_data_this_status_repl_id %>%
                filter(reply_to_status_id %in% .status_id_this_i) %>%
                pull(status_id)
              .status_id_this <- append(.status_id_this, .status_id_this_i)
            }
            .status_id_this <- unique(.status_id_this)
            return(.status_id_this)
          }, .workers=.workers)) %>%
          as_tibble() %>%
          mutate(thread_id = as.character(thread_id),
                 status_id = map(status_id, as.character)) %>%
          unnest_longer(status_id) %>% 
          drop_na()
        
        setup_disk.frame(workers=.workers)
        
        if(nrow(.thread_status_id_this) > 0){
          
          .repl_data_this <-
            .repl_data_this %>% 
            delayed(function(..chunk){
              if(rlang::has_name(..chunk, "status_id")){
                return(inner_join(..chunk, .thread_status_id_this, 
                                  by="status_id"))
              }else{
                return(slice(..chunk, 0))
              }
            }) %>% 
            write_disk.frame(outdir=tmp_df())
          
        }
        
      }
      
      ####
      
      .repl_data <-
        list(.repl_data_this, .repl_data_last) %>% 
        rbindlist.disk.frame(outdir=glue("{.screen_name_dir}/repl.df"),
                             compress=50, overwrite=TRUE, .progress=FALSE)
      
      invisible(cmap(.repl_data, function(..chunk){
        invisible(..chunk); rm(..chunk); gc(); return(NULL)
      }, lazy=FALSE))
      
      gc()
      
    }else{
      
      .repl_max_id <- .repl_since_id_this
      
      .repl_data <- pluck(.transp_r, "tweets", "repl_data")
      
    }
    
    ##################
    # PREPARE OUTPUT #
    ##################
    
    .tweets <- 
      list(from_data = .from_data, repl_data = .repl_data,
           from_max_id = .from_max_id, repl_max_id = .repl_max_id)
    
    tweet_info_env[["tweets_count_stored"]] <- 
      tweet_info_env[["tweets_count_stored"]] +
      nrow(.from_data) + nrow(.repl_data)
    
    tweet_info_env[["tweets_count_collected_this"]] <- 
      tweet_info_env[["tweets_count_collected_this"]] +
      nrow(.from_data_this) + nrow(.repl_data_this)
    
  }, error = function(.error){
    
    .tweets <<- pluck(.transp_r, "tweets")
    message("Error at ", .transp_r$screen_name, ": ", .error$message)
    
  }, finally = {
    
    setup_disk.frame(workers=1)
    gc()
    
  })

  return(.tweets)
  
}



