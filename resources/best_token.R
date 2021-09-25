if(!"naptime" %in% installed.packages()){install.packages("naptime")}

`%>%` <- magrittr::`%>%`
# 
# .token_list <- readr::read_rds("~/Documents/uni/masterarbeit/analysis/resources/token_list.rds")
# .query <- "search/tweets"

################################################################################
################################################################################
################################################################################

best_token <- function(.query="search/tweets", .token_list=token_list){
  
  .best_token <- NULL
  
  while(is.null(.best_token)){
    
    try({
      
      .token_df <- NULL
      
      while(all(purrr::map_lgl(purrr::pluck(.token_df, "rate_limit"), is.null))){
        
        .token_df <- 
          tibble::tibble(token = sample(.token_list)) %>% 
          dplyr::mutate(rate_limit = purrr::map(token, function(.token){
            .rate_limit <- NULL
            tryCatch({
              .rate_limit <- rtweet::rate_limit(token=.token, query=.query)
            }, error=function(.error){
              .error_is_connection <- 
                as.character(.error) %>% 
                str_detect("(?i)(TIMEOUT WAS REACHED)|(COULD NOT RESOLVE HOST)")
              if(.error_is_connection){
                warning(Sys.time(), " - cannot connect", call.=FALSE)
                Sys.sleep(30)
              }
              .rate_limit <<- NULL
            }, warning = function(.warning){
              .warning_is_ratelimit <- 
                as.character(.warning) %>% 
                str_detect("(?i)RATE[- ]?LIMIT")
              if(.warning_is_ratelimit){
                .rate_limit <<- NULL
              }
            })
            return(.rate_limit)
          }))
        
        if(all(purrr::map_lgl(purrr::pluck(.token_df, "rate_limit"), is.null))){
          message("Cannot determine rate limit reset timers. Retrying in 30s")
          naptime::naptime(lubridate::seconds(30))
        }
        
      }
      
      .token_df <- tidyr::unnest(.token_df, rate_limit)#; .token_df
      
      if(all(.token_df$remaining == 0)){
        
        message("Waiting for rate limit resets at ", min(.token_df$reset_at))
        naptime::naptime(min(.token_df$reset_at) + lubridate::seconds(1))
        
      }else{
        
        .best_token <- 
          .token_df %>% 
          dplyr::arrange(desc(remaining)) %>% 
          dplyr::filter(remaining > 0) %>% 
          purrr::pluck("token", 1)
        
      }
      
    })
  
  }
  
  return(.best_token)
  
}

