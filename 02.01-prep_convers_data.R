if(grepl("(?i)linux", sessionInfo()$running)){
  setwd("~/Documents/uni/masterarbeit/analysis")
}else{
  setwd("I:/masterarbeit/analysis")
}

if(!"unixtools" %in% installed.packages()){
  remotes::install_github("s-u/unixtools")
}

options(parallelly.makeNodePSOCK.setup_strategy = "sequential")

# remotes::install_github("HenrikBengtsson/future")

library(glue)
library(fs)
library(tidyverse)
library(magrittr)
library(lubridate)
library(furrr)
library(disk.frame)
library(bit64)
library(textclean)
library(quanteda)

source("./resources/disk.frame_extensions.R")

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

workers <- 2

options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=workers)
quanteda::quanteda_options(threads=workers)
fst::threads_fst(1)

data_dir <- path_wd("data")
temp_dir <- set_temp_dir(path(data_dir, "tmp"))

################################################################################
################################################################################
################################################################################

election_day <- ymd("2020-11-03", tz="EST")
study_tscope <- interval(election_day-weeks(10), ymd("2021-01-06", tz="EST"))

################################################################################
################################################################################
################################################################################

keep_cols_regex <- 
  "^(text|created_at|((reply_to_)?status|convers|user)_id|lang)$"

hash_is_tag_regex <- 
  "(?<![[:alnum:]&])#[[:alpha:]][[:alnum:]_]*(?=([^[:alnum:]_]|$))"
hash_not_tag_regex <- 
  "(?<=[[:alnum:]&])#|#(?![[:alpha:]])"
at_is_user_regex <- 
  "(?<![[:alnum:]&])@[[:alnum:]_]+(?=([^[:alnum:]_]|$))"
at_not_user_regex <- 
  "(?<=[[:alnum:]&])@|@(?![[:alnum:]_])"

quote_apost_chars_regex <- 
  "(\\u0022|\\u0027|\\u0060|\\u00B4|\\u2018|\\u2019|\\u201C|\\u201D)"
dash_chars_regex <- 
  "(\\u2013|\\u2014|\\u2212|\\u002D)"

latin_letters_regex <- read_rds("./resources/latin_letters_regex.rds")

prefix_dash_regex <-
  read_rds("./resources/prefix_table.rds") %>% 
  mutate(prefix = str_remove_all(prefix, dash_chars_regex)) %>% 
  glue_data("(?<=({str_c(prefix, collapse=\"|\")}))-(?=[a-z])")

digits_not_tag_user_regex <- " ?(?<!(?:^| )[@#][a-z0-9_]{0,280})([0-9]+) ?"
uscore_not_tag_user_regex <- " ?(?<!(?:^| )[@#][a-z0-9_]{0,280})(_+) ?"

source("./resources/get_number_regex.R")

sword <- stopwords("en", "nltk")
sword_margin_regex <- 
  glue("(?i)(^({str_c(sword, collapse='|')}) )|",
       "( ({str_c(sword, collapse='|')})$)")

rep_phrase_regex <- 
  str_c("(?<=(?:^| ))((?:[[:graph:]<>]+[^[:graph:]]+){0,9}?[[:graph:]]+)",
        "(?:[ [:punct:]]+\\1)+(?=(?: |$))")

lemma_dictionary <-
  lexicon::hash_lemmas %>% 
  filter(token != "pence" & !str_detect(lemma, "\\d"))

################################################################################
################################################################################
################################################################################

tweets_raw_prep <- read_rds("./data/02-tweets_raw_prep.rds")
# tweets_raw_prep <-
#   path(data_dir, "01-tweets_raw") %>%
#   dir_ls(type="file", regexp="/\\d{4}\\.rds$") %>%
#   last() %>%
#   read_rds() %>%
#   mutate(tweets = map(transpose(.), function(.transp_r){
# 
#     .root_path <- path_abs("./data/02-tweets_raw_prep")
#     .screen_name_path <- path(.root_path, pluck(.transp_r, "screen_name"))
#     .from_data_mod_path <- path(.screen_name_path, "from_data")
#     .repl_data_mod_path <- path(.screen_name_path, "repl_data")
# 
#     .from_data <-
#       .transp_r %>%
#       pluck("tweets", "from_data") %>%
#       collect() %>%
#       mutate(across(matches("_id$"), as.integer64)) %>% 
#       distinct(status_id, .keep_all=TRUE)
#     
#     reset_workers_df()
#     
#     .thread_status_id_dict <-
#       .from_data %>%
#       filter(is.na(reply_to_status_id) & status_id %in% reply_to_status_id) %>%
#       rename(thread_id = status_id) %>%
#       mutate(status_id_list = map(thread_id, function(.thread_id){
#         
#         .status_id_this <- character(0)
#         .status_id_this_i <- .thread_id
#         
#         while(length(.status_id_this_i) > 0){
#           .status_id_this_i <-
#             .from_data %>%
#             filter(reply_to_status_id %in% .status_id_this_i) %>%
#             pull(status_id)
#           .status_id_this <- append(.status_id_this, .status_id_this_i)
#         }
#         
#         .status_id_this <- c(.thread_id, .status_id_this)
#         
#         return(.status_id_this)
#         
#       })) %>%
#       select(thread_id, status_id_list) %>%
#       unnest_longer(status_id_list, values_to="status_id") %>%
#       left_join(select(.from_data, status_id, created_at),
#                 by="status_id") %>%
#       group_by(thread_id) %>%
#       arrange(status_id) %>%
#       filter(created_at < first(created_at) + minutes(5)) %>%
#       ungroup() %>%
#       select(thread_id, status_id)
# 
#     reset_workers_df()
#     
#     .from_data_thread_collapse <-
#       .thread_status_id_dict %>%
#       inner_join(.from_data, by="status_id") %>%
#       group_by(thread_id) %>%
#       arrange(status_id) %>%
#       summarise(text = str_c(str_trim(text), collapse=" <<<>>> "),
#                 thread_length = length(status_id),
#                 across(-text, first)) %>%
#       select(-thread_id)
# 
#     reset_workers_df()
#     
#     .from_data_mod <-
#       .from_data %>%
#       anti_join(select(.thread_status_id_dict, status_id),
#                 by="status_id") %>%
#       bind_rows(.from_data_thread_collapse) %>%
#       arrange(status_id) %>%
#       as.disk.frame(outdir=.from_data_mod_path, overwrite=TRUE,
#                     nchunks=nchunks(pluck(.transp_r, "tweets", "from_data")))
# 
#     reset_workers_df()
#     
#     .repl_data_mod <-
#       .transp_r %>%
#       pluck("tweets", "repl_data") %>% 
#       cmap(function(..chunk){
#         
#         ..chunk_mod <-
#           ..chunk %>%
#           rename(convers_id = thread_id) %>%
#           mutate(across(matches("_id$"), as.integer64)) %>%
#           filter(!status_id %in% .from_data$status_id) %>%
#           mutate(across(c(reply_to_status_id, convers_id), plyr::mapvalues,
#                         from=.thread_status_id_dict$status_id,
#                         to=.thread_status_id_dict$thread_id,
#                         warn_missing=FALSE))
#         
#         return(..chunk_mod)
#         
#       }, lazy=FALSE, overwrite=TRUE, outdir=.repl_data_mod_path)
#     
#     reset_workers_df()
#     
#     .tweets_mod <-
#       .transp_r %>%
#       pluck("tweets") %>%
#       assign_in("from_data", .from_data_mod) %>%
#       assign_in("repl_data", .repl_data_mod)
# 
#     return(.tweets_mod)
# 
#   })) %>%
#   reset_workers_df() %>% 
#   write_rds(path(data_dir, "02-tweets_raw_prep.rds"))

################################################################################
################################################################################
################################################################################

convers_info <- read_rds("./data/02-convers_info.rds")

# convers_meta <-
#   tweets_raw_prep %>%
#   mutate(from_data = map(tweets, "from_data")) %>%
#   pull(from_data) %>%
#   map_dfr(collect) %>%
#   as_tibble() %>%
#   distinct(status_id, .keep_all=TRUE) %>%
#   filter(!is_retweet) %>%
#   filter(is.na(reply_to_status_id)) %>%
#   mutate(across(matches("_created_at_"), `tz<-`, "EST")) %>%
#   filter(created_at %within% study_tscope) %>%
#   rename(convers_id = status_id) %>%
#   mutate(across(matches("_id$"), as.integer64))
# 
# convers_repl_counts <-
#   tweets_raw_prep %>%
#   transmute(repl_data = map(tweets, "repl_data")) %>%
#   mutate(repl_data = map(repl_data, function(.repl_data){
#     .repl_data %>%
#       srckeep(c("convers_id", "lang")) %>%
#       cmap_dfr(function(..chunk){
#         ..chunk %>%
#           mutate(across(matches("_id$"), as.integer64)) %>%
#           filter(convers_id %in% convers_meta$convers_id & lang == "en") %>%
#           count(convers_id, name="convers_repl_count")
#       }) %>%
#       group_by(convers_id) %>%
#       summarise(convers_repl_count = sum(convers_repl_count), .groups="drop")
#   })) %>%
#   unnest(repl_data)
# 
# setup_disk.frame(workers=1); plan(multisession, workers=25)
# 
# convers_info <-
#   convers_meta %>%
#   full_join(convers_repl_counts, by="convers_id") %>%
#   transmute(convers_id, convers_candidate=screen_name,
#             convers_start_at=with_tz(created_at, "EST"), 
#             convers_start_text=text,
#             convers_start_thread_length=replace_na(thread_length, 1),
#             convers_size_raw=convers_repl_count+1,
#             convers_start_quoted_text=quoted_text,
#             convers_start_quoted_screen_name=quoted_screen_name) %>%
#   mutate(convers_start_url_type = future_map(convers_start_text, function(.text){
# 
#     .url <-
#       .text %>%
#       str_extract_all("https://t.co/[[:alnum:]]+") %>%
#       chuck(1) %>%
#       as_tibble_col("url_short")
# 
#     if(!isTRUE(nrow(.url) > 0)){return(NULL)}
# 
#     .url_type <-
#       .url %>%
#       mutate(url_long = map_chr(url_short, possibly(function(..url){
#         curl::curl_fetch_memory(..url, handle=curl::new_handle(nobody=TRUE))$url
#       }, otherwise=NA_character_))) %>%
#       mutate(url_is_twitter = str_detect(url_long, "^https://twitter.com/")) %>%
#       mutate(url_media_twitter = map_chr(url_long, possibly(function(..url){
#         str_match(..url, "/[[:digit:]]+/(photo|video)/[[:digit:]]+$")[1,2]
#       }, otherwise=NA_character_))) %>%
#       mutate(url_type = case_when(
#         url_is_twitter & !is.na(url_media_twitter) ~
#           str_c("Twitter ", url_media_twitter),
#         url_is_twitter & is.na(url_media_twitter) ~
#           "Twitter other",
#         !url_is_twitter ~
#           "external"
#       )) %>%
#       pull(url_type)
# 
#       return(.url_type)
# 
#   }, .progress=TRUE)) %>%
#   write_rds(path(data_dir, "02-convers_info.rds"))
# 
# plan(sequential); setup_disk.frame(workers=workers)

################################################################################
################################################################################
################################################################################

convers_full_raw <- disk.frame(path(data_dir, "02-convers_full_raw.df"))
# convers_full_raw <-
#   tweets_raw_prep %>%
#   mutate(tweets = map(tweets, function(.tweets){
#     magrittr::extract(.tweets, str_subset(names(.tweets), "_data$"))
#   })) %>%
#   unnest_longer(tweets, values_to="data", indices_to="source") %>%
#   pull(data) %>%
#   rbindlist.disk.frame(outdir=tmp_df()) %>%
#   reset_workers_df() %>%
#   resuming_cmap(function(.chunk){
#     .chunk <-
#       .chunk %>%
#       select(matches(keep_cols_regex)) %>%
#       mutate(across(matches("_id$"), as.integer64)) %>%
#       mutate(across(matches("\\bcreated_at\\b"), with_tz, "EST"))
#     if("convers_id" %in% colnames(.chunk)){
#       .chunk$convers_id[is.na(.chunk$convers_id)] <-
#         .chunk$status_id[is.na(.chunk$convers_id)]
#     }else{
#       .chunk$convers_id <- .chunk$status_id
#     }
#     .chunk <- filter(.chunk, convers_id %in% convers_info$convers_id)
#     return(.chunk)
#   }, .outdir=tmp_df(), .workers=workers, .scheduling=10) %>%
#   reset_workers_df() %>%
#   rechunk_df(.nrow_chunks=5e4L, .outdir=tmp_df()) %>%
#   reset_workers_df() %>%
#   hard_group_by(convers_id, overwrite=TRUE, outdir=tmp_df()) %>% 
#   resuming_cmap(function(.chunk){
#     distinct(.chunk, status_id, .keep_all=TRUE)
#   }, .outdir=tmp_df(), .workers=workers, .scheduling=10) %>% 
#   copy_df_to(outdir=path(data_dir, "02-convers_full_raw.df"))

# tmp_df(.mode="clear_old")
reset_workers_df()

################################################################################
################################################################################
################################################################################

reset_workers_df(.change_workers=workers)

# convers_full_clean <-
#   disk.frame(path(data_dir, "02-convers_full_raw.df")) %>% 
#   reset_workers_df() %>% 
#   resuming_cmap(function(.chunk){
# 
#     .chunk_mod <-
#       .chunk %>% 
#       tibble::as_tibble() %>%
#       dplyr::filter(lang == "en") %>%
#       dplyr::select(convers_id, status_id, created_at, text) %>%
#       tidyr::drop_na() %>%
#       dplyr::mutate(
#         text =
#           text %>%
#           # REMOVE THE MENTIONS FROM THE BEGINNING OF EACH TWEET (REPLY TO ...)
#           stringr::str_remove(glue::glue("^({at_is_user_regex} *)*")) %>%
#           # PERFORM SOME VERY BASIC STING PROCESSIONG
#           stringr::str_replace_all("[^[:graph:]]+", " ") %>%
#           textutils::HTMLdecode() %>%
#           textclean::replace_url(replacement=" ~~~ ") %>%
#           # REMOVE ALL EXTENSIONS FROM LATIN CHARACTERS
#           map_chr(function(..string){
#             walk(transpose(latin_letters_regex), function(...l){
#               ..string <<-
#                 str_replace_all(..string, ...l$pattern, ...l$replacement)
#             })
#             return(..string)
#           }) %>%
#           # UNIFY THE USE OF DASH AND BACKETS CHARACTERS
#           stringr::str_replace_all(quote_apost_chars_regex, "'") %>%
#           stringr::str_replace_all(dash_chars_regex, "-") %>%
#           stringr::str_squish()
#       ) %>%
#       data.table::as.data.table()
# 
#     return(.chunk_mod)
# 
#     at_is_user_regex
# 
#   }) %>%
#   reset_workers_df() %>% 
#   rechunk_df(.nrow_chunks=5e4L, .outdir=tmp_df()) %>%
#   hard_group_by(convers_id, overwrite=TRUE,
#                 outdir=path(data_dir, "02-convers_full_clean.df"))

# tmp_df(.mode="clear_old"); reset_workers_df()

################################################################################
################################################################################
################################################################################

# convers_full_tokens <-
#   disk.frame(path(data_dir, "02-convers_full_clean.df")) %>%
#   reset_workers_df(.change_workers=workers) %>%
#   resuming_cmap(function(.chunk){
# 
#     .chunk_mod <-
#       .chunk %>%
#       tibble::as_tibble() %>%
#       dplyr::mutate(
#         text =
#           text %>%
#           stringr::str_to_lower() %>%
#           # HANDLE INTERNET SPECIFIC TEXT FORMS
#           textclean::replace_internet_slang() %>%
#           textclean::replace_word_elongation() %>%
#           # HANDLE SYMBOLS
#           stringr::str_replace_all(" *& *", " and ") %>%
#           stringr::str_replace_all(" *\\$ *", " dollar ") %>%
#           stringr::str_replace_all(" *% *", " percent ") %>%
#           stringr::str_replace_all(at_not_user_regex, " at ") %>%
#           stringr::str_replace_all(hash_not_tag_regex, " number ") %>%
#           # HANDLE CONTRACTIONS, GENITIVE, APOSTROPHE, QUOTE
#           textclean::replace_contraction() %>%
#           stringr::str_replace_all("(?<=[a-z0-9])'s\\b", "") %>%
#           # HANDLE DOTTED ACRONYMS AND NUMBER SEPARATORS (DECIMAL & BIG)
#           stringr::str_remove_all("(?<=[^a-z][a-z])\\.") %>%
#           stringr::str_remove_all("(?<=\\[0-9])[.,]+(?=[0-9])") %>%
#           # REMOVE UNWANTED SYMBOLS
#           stringr::str_replace_all(" *['] *", " ") %>%
#           stringr::str_replace_all("(?<=[a-z0-9])-(?=[a-z0-9])", " ") %>%
#           stringr::str_replace_all("[^[a-z0-9]#@_ ]", " . ") %>%
#           stringr::str_replace_all(uscore_not_tag_user_regex, " . ") %>%
#           stringr::str_replace_all("(?: *\\. *)+", " . ") %>%
#           # SPELL OUT ORDINAL, BLUR OTHER NUMBERS
#           textclean::replace_ordinal() %>%
#           stringr::str_replace_all(digits_not_tag_user_regex, " \\1 ") %>%
#           stringr::str_replace_all(number_regex, " <num> ") %>%
#           stringr::str_replace_all(ordinal_regex, " <num> ") %>%
#           stringr::str_replace_all("(?: *<num> *)+", " <num> ") %>%
#           # REMOVE REPETITIONS OF PHRASES
#           stringr::str_replace_all("(?<!([#@]|\\<))\\b(?!( |\\>))", " ") %>%
#           stringr::str_replace_all(rep_phrase_regex, "\\1") %>%
#           # LEMMATIZE WORDS
#           stringr::str_split(" +") %>%
#           purrr::map_chr(function(..tokens){
#             ..tokens %>%
#               textstem::lemmatize_words(lemma_dictionary) %>%
#               stringr::str_c(collapse=" ")
#           }) %>%
#           stringr::str_remove_all("(^[. ]*)|([. ]*$)")
#       ) %>%
#       data.table::as.data.table()
# 
#     gc()
# 
#     return(.chunk_mod)
# 
#   }) %>%
#   reset_workers_df() %>% 
#   write_disk.frame(outdir=path(data_dir, "02-convers_full_token.df"),
#                    overwrite=TRUE)

reset_workers_df()

################################################################################
################################################################################
################################################################################

# convers_colloc_sample_status_id <-
#   disk.frame(path(data_dir, "02-convers_full_token.df")) %>%
#   reset_workers_df() %>%
#   srckeep(c("convers_id", "status_id")) %>%
#   cmap_dfr(distinct) %>%
#   left_join(transmute(convers_info, convers_candidate, convers_id),
#             by="convers_id") %>%
#   group_by(convers_candidate) %>%
#   slice_sample(n=5e5) %>%
#   ungroup() %>%
#   pull(status_id) %>%
#   reset_workers_df()
# 
# convers_colloc_sample_text <-
#   disk.frame(path(data_dir, "02-convers_full_token.df")) %>%
#   reset_workers_df() %>%
#   srckeep(c("status_id", "text")) %>%
#   cmap_dfr(function(.chunk){
#     filter(distinct(.chunk, status_id, .keep_all=TRUE),
#            status_id %in% convers_colloc_sample_status_id)
#   }) %>% 
#   pull(text) %>%
#   reset_workers_df()

convers_colloc <- read_rds(path(data_dir, "02-convers_colloc.rds"))
# convers_colloc <- 
#   convers_colloc_sample_text %>% 
#   discard(is.na) %>% 
#   str_split(" ") %>%
#   tokens() %>% 
#   quanteda.textstats::textstat_collocations(min_count=500, size=2:5) %>%
#   reset_workers_df(.change_workers=workers) %>%
#   write_rds(path(data_dir, "02-convers_colloc.rds"))

convers_colloc_candidate <-
  convers_colloc %>%
  as_tibble() %>%
  rename(colloc = collocation) %>%
  # FILTER OUT INSIGNIFICANT COLLOCATIONS
  filter(count >= 500) %>%
  filter(z >= 5) %>%
  # REMOVE COLLOCATIONS STARTING OR ENDING WITH STOPWORDS
  filter(!str_detect(colloc, sword_margin_regex)) %>%
  # REMOVE COLLOCATIONS STARTING OR ENDING WITH DUPLICATE WORDS
  filter(!str_detect(colloc, "^([^ ]+) \\1(?: |$)")) %>%
  filter(!str_detect(colloc, "(?:^| )([^ ]+) \\1$")) %>%
  # REMOVE COLLOCATIONS CONTAINING 2 HASHTAGS OR MENTIONS IN A ROW
  filter(!str_detect(colloc, "([@#])[^ ]+ \\1[^ ]+")) %>%
  # ...
  mutate(colloc_regex = glue("(?<=(^| )){colloc}(?=( |$))"))

################################################################################
# RECURSIVELY FILTER OUT COLLOCATIONS THAT ARE MAINLY SUBSTRINGS OF LONGER ONES

for(len_i in sort(unique(convers_colloc_candidate$length), decreasing=TRUE)){

  if(len_i == max(convers_colloc_candidate$length)){

    convers_colloc_select <- filter(convers_colloc_candidate, length == len_i)

  }else{

    convers_colloc_i <-
      convers_colloc_candidate %>%
      filter(length == len_i) %>%
      mutate(colloc_sub_count = map_int(colloc_regex, function(.colloc_regex){
        .match <- str_detect(convers_colloc_select$colloc, .colloc_regex)
        sum(convers_colloc_select$count[.match])
      })) %>%
      filter(colloc_sub_count / count < 0.75)

    convers_colloc_select <- bind_rows(convers_colloc_select, convers_colloc_i)

  }

  if(len_i == min(convers_colloc_candidate$length)){

    convers_colloc_pattern <-
      convers_colloc_select %>%
      arrange(-length, -z) %>%
      pull(colloc) %>%
      phrase()

  }

}; rm(len_i, convers_colloc_select, convers_colloc_i)

################################################################################

convers_full_term_count <-
  disk.frame(path(data_dir, "02-convers_full_token.df")) %>%
  reset_workers_df(.change_workers=workers) %>%
  resuming_cmap(function(.chunk){
    
    quanteda_options(threads=1)
    
    .chunk_mod <-
      .chunk %>%
      dplyr::distinct(status_id, .keep_all=TRUE) %>% 
      tidyr::drop_na(text) %>% 
      dplyr::mutate(
        text =
          text %>%
          stringr::str_split(" ") %>%
          quanteda::tokens() %>%
          (function(..tokens){
            for(..pattern in convers_colloc_pattern){
              ..tokens <- quanteda::tokens_compound(
                ..tokens, list(..pattern), concatenator=" ",
                valuetype="fixed", join=FALSE
              )
            }; rm(..pattern)
            return(..tokens)
          }) %>%
          quanteda::tokens_remove(sword, valuetype="fixed") %>%
          quanteda::tokens_keep("[a-z]+", valuetype="regex") %>%
          quanteda::tokens_remove("^([a-z]|<num>)$", valuetype="regex") %>%
          as("list")) %>%
      tidyr::unnest_longer(text, values_to="term") %>%
      tidyr::drop_na() %>%
      dplyr::count(dplyr::across(tidyselect::everything()), name="count") %>%
      data.table::as.data.table()

    gc()

    return(.chunk_mod)

  }) %>%
  rechunk_df(.nrow_chunks=5e5L, .outdir=tmp_df()) %>%
  hard_group_by(convers_id, overwrite=TRUE,
                outdir=path(data_dir, "02-convers_full_term_count.df"))

reset_workers_df()

################################################################################
################################################################################
################################################################################

convers_full_aggr_term_count <-
  disk.frame(path(data_dir, "02-convers_full_term_count.df")) %>%
  reset_workers_df(.change_workers=workers) %>%
  resuming_cmap(function(.chunk){

    .chunk_mod <-
      .chunk %>%
      tidyr::drop_na() %>%
      dplyr::group_by(convers_id, term) %>%
      dplyr::summarise(count = sum(count), .groups="drop_last") %>%
      dplyr::mutate(share = count/sum(count)) %>%
      dplyr::ungroup() %>%
      data.table::as.data.table()

    return(.chunk_mod)

  }) %>%
  reset_workers_df() %>%
  rechunk_df(.nrow_chunks=5e5L, .outdir=tmp_df()) %>%
  hard_group_by(term, overwrite=TRUE,
                outdir=path(data_dir, "02-convers_full_aggr_term_count.df"))

reset_workers_df()

################################################################################
################################################################################
################################################################################

# terms_max_share <- read_rds(path(data_dir, "02-terms_max_share.rds"))
terms_max_share <-
  disk.frame(path(data_dir, "02-convers_full_aggr_term_count.df")) %>%# get_chunk(1) -> .chunk
  reset_workers_df(.change_workers=workers) %>%
  cmap_dfr(function(.chunk){
    
    .convers_candidate_count <- 
      n_distinct(dplyr::pull(convers_info, convers_candidate))
    
    .convers_candidate_convers_count <- 
      dplyr::summarise(dplyr::group_by(convers_info, convers_candidate), 
                       convers_count = dplyr::n_distinct(convers_id))
    
    .chunk_term_shares <- 
      .chunk %>% 
      dplyr::left_join(
        dplyr::select(convers_info, convers_id, convers_candidate),
        by="convers_id"
      ) %>% 
      dplyr::group_by(term, convers_candidate) %>% 
      dplyr::summarise(share = sum(share), .groups="drop") %>% 
      dplyr::left_join(.screen_name_convers_count, by="convers_candidate") %>% 
      dplyr::mutate(share = share / convers_count) %>% 
      dplyr::group_by(term) %>% 
      dplyr::summarise(share = sum(share)/.convers_candidate_count, 
                       .groups="drop")

    return(.chunk_term_shares)
    
  }) %>% 
  reset_workers_df(.change_workers=1) %>% 
  slice_max(share, n=1e5L, with_ties=FALSE) %>%
  pull(term) %>% 
  write_rds(path(data_dir, "02-terms_max_share.rds")) %>% 
  reset_workers_df(.change_workers=workers)

# Tokens zählen
disk.frame(path(data_dir, "02-convers_full_aggr_term_count.df")) %>% 
  cmap_dfr(~{summarise(group_by(.x, term), count=sum(count))}) %>% 
  group_by(term) %>% 
  summarise(count=sum(count)) %T>% 
  {cat("Unterschiedliche Tokens: ", nrow(.), "", sep="\n")} %>% 
  arrange(-count) %>% 
  mutate(is_select = row_number() <= 1e4) %T>% 
  {cat("Geringste Häufigkeit in Auswahl: ", min(.$count[.$is_select]), "", 
       sep="\n")} %>% 
  group_by(is_select) %>% 
  summarise(count=sum(count), .groups="drop") %>% 
  mutate(share = count/sum(count)) %T>% 
  {cat("Anzahl und Anteil nach Auswahl:", capture.output(.), "", sep="\n")} %>% 
  summarise(count = sum(count)) %T>% 
  {cat("Anzahl insgesamt:", capture.output(.), "", sep="\n")} %>% 
  invisible()

################################################################################
## CREATE STM-DATA #############################################################
################################################################################

terms_keep <- head(terms_max_share, 1e4)

convers_stm_data <- 
  disk.frame(glue("{data_dir}/02-convers_full_aggr_term_count.df")) %>% 
  reset_workers_df(.change_workers=workers) %>% 
  cmap_dfr(function(.chunk){filter(.chunk, term %in% terms_keep)}) %>% 
  reset_workers_df(.change_workers=1) %>% 
  drop_na() %>% 
  tidytext::cast_dfm(document=convers_id, term=term, value=count) %>% 
  magrittr::extract(1:nrow(.), terms_keep) %>% 
  quanteda::convert(to="stm") %>% 
  reset_workers_df(.change_workers=workers)

convers_stm_data$meta <- 
  tibble(convers_id = names(convers_stm_data$documents)) %>% 
  left_join(mutate(convers_info, convers_id = as.character(convers_id)), 
            by="convers_id") %>% 
  select(convers_id, screen_name, text, created_at) %>% 
  mutate(created_at_num = 
           created_at %>% 
           as.integer() %>% 
           subtract(min(., na.rm=TRUE)) %>% 
           divide_by(max(., na.rm=TRUE)),
         party = 
           screen_name %>% 
           is_in(c("realDonaldTrump", "Mike_Pence")) %>% 
           if_else("Republican", "Democratic", NA_character_)) %>% 
  as.data.frame()

write_rds(convers_stm_data, "./data/02-convers_stm_data.rds")

setup_disk.frame(workers=1)

################################################################################
################################################################################
################################################################################

setup_disk.frame(workers=5)

convers_stm_vocab <- 
  chuck(read_rds(path(data_dir, "02-convers_stm_data.rds")), "vocab")

status_id_has_term_count_and_vocab <-
  disk.frame(path(data_dir, "02-convers_full_term_count.df")) %>%
  reset_workers_df() %>%
  cmap_dfr(function(.chunk){
    
    .chunk_mod <-
      .chunk %>%
      mutate(is_vocab = term %in% convers_stm_vocab) %>%
      group_by(status_id) %>%
      summarise(term_count = sum(count),
                term_count_is_vocab = sum(count[is_vocab]),
                .groups="drop") %>% 
      filter(term_count >= 3, term_count/term_count_is_vocab >= 2/3)
    
    return(.chunk_mod)
    
  }) %>%
  reset_workers_df() %>%
  distinct(status_id) %>% 
  write_rds(path(data_dir, "02-status_id_has_term_count_and_vocab.rds"))

setup_disk.frame(workers=1)

################################################################################
################################################################################
################################################################################

convers_full_net_stat <- 
  disk.frame(path(data_dir, "02-convers_full_raw.df")) %>% 
  reset_workers_df() %>% 
  resuming_cmap(function(.chunk){
    
    library(tidygraph)
    
    .chunk_mod <- 
      .chunk %>% 
      group_by(convers_id) %>% 
      group_modify(function(.gdata, .gkey){
        
        .reply_times <- 
          .gdata %>% 
          select(status_id, parent_id = reply_to_status_id, 
                 status_created_at = created_at) %>% 
          (function(..data){
            left_join(..data,
                      select(..data, parent_id = status_id, 
                             parent_created_at = status_created_at),
                      by="parent_id")
          }) %>% 
          mutate(
            status_replied_after_seconds = 
              time_length(status_created_at - parent_created_at, "seconds")
          ) %>% 
          select(status_id, status_replied_after_seconds)
        
        .gdata_mod <- 
          .gdata %>% 
          select(to=status_id, from=reply_to_status_id) %>% 
          mutate(across(everything(), as.character)) %>% 
          drop_na() %>% 
          distinct() %>% 
          as_tbl_graph() %>% 
          activate(nodes) %>% 
          mutate(
            status_dist_from_convers_start = node_distance_from(node_is_root()),
            status_reply_count_direct = centrality_degree(mode="out"),
            status_reply_count_all = 
              map_dfs_back_dbl(node_is_root(), .f=function(path, ...){
                length(path$parent)
              })
          ) %>% 
          arrange(status_dist_from_convers_start, -status_reply_count_all) %>% 
          as_tibble() %>% 
          mutate(across(name, as.integer64)) %>% 
          rename(status_id = name) %>% 
          left_join(.reply_times, by="status_id")
        
        return(.gdata_mod)
        
      }) %>% 
      ungroup()
    
    gc()
    
    return(.chunk_mod)
    
  }, .workers=5, .scheduling=Inf, .resume=TRUE) %>% 
  reset_workers_df() %>% 
  write_disk.frame(outdir=path(data_dir, "02-convers_full_net_stat.df"),
                   overwrite=TRUE)

setup_disk.frame(1); fst::threads_fst(6)
