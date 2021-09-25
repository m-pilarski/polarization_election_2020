
# MODIFY A FUNCTION SO THAT IT REEXECUTES IF AN ERROR IS THROWN THAT CONTAINS 
# "TIMEOUT WAS REACHED" OR "COULD NOT RESOLVE HOST". IF AN OTHER ERROR IS THROWN
# RETURN THAT ERROR OR STOP (depending on .return_error)

with_retries <- function(.function, .return_error=FALSE){
  .function_mod <- function(...){
    .result <- NULL; while(is.null(.result)){
      tryCatch({
        .result <- .function(...)
        if(is.null(.result)){stop("function returned NULL")}
      }, error=function(.error){
        .error_is_connection <- 
          as.character(.error) %>% 
          str_detect("(Timeout was reached)|(Could not resolve host)")
        if(.error_is_connection){
          warning(Sys.time(), " - cannot connect", call.=FALSE)
          .result <<- NULL
          Sys.sleep(30)
        }else if(.return_error){
          warning(.error)
          .result <<- .error
        }else{
          stop(.error)
        }
      })
    }
    return(.result)
  }
  return(.function_mod)
}
