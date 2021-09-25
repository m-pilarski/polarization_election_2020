make_number_regex <- function(){
  
  .reg_1_9 <- 
    glue::glue("(?:f(?:ive|our)|s(?:even|ix)|t(?:hree|wo)|(?:ni|o)ne|eight)")
  
  .reg_10_19 <- 
    glue::glue("(?:(?:(?:s(?:even|ix)|f(?:our|if)|nine)te|e(?:ighte|lev))en|",
               "t(?:(?:hirte)?en|welve))")
  
  .reg_2_9x10 <- 
    glue::glue("(?:(?:s(?:even|ix)|t(?:hir|wen)|f(?:if|or)|eigh|nine)ty)")
  
  .reg_1_99 <- 
    glue::glue("(?:{.reg_2_9x10}[- ]?{.reg_1_9}?|{.reg_10_19}|{.reg_1_9})")
  
  .reg_1_999 <- 
    glue::glue("(?:(?:{.reg_1_9}) hundred(?: (?:and )?(?:{.reg_1_99}))?|",
               "(?:{.reg_1_99}))")
  
  .reg_big_l <- 
    glue::glue("(?:hundred|thousand|(?:m|b|tr)illion)")
  
  .reg_big_s <- 
    glue::glue("(?:(?:k|thsnd|m(?:ill?|ln)?|b(?:ill?|l?n)?|",
               "t(?:r(?:ill?|l?n))?)\\.?)")
  
  .reg_num_start <-
    glue::glue("(?:zero|{.reg_1_999}|{.reg_big_l}s?|[0-9][0-9.,]*)")
  # .reg_num_start <-
  #   glue::glue("(?:-? ?[0-9][0-9.,]*|{.reg_big_l}s?)")
  # .reg_num_start <-
  #   glue::glue("(?:-? ?[0-9][0-9.,]*)")
  
  .reg_num_rest <-
    glue::glue("(?:{.reg_num_start}|{.reg_big_s})")
  # .reg_num_rest <-
  #   glue::glue("(?:{.reg_num_start}|{.reg_big_l}|{.reg_big_s})")

  # .reg_num <- 
  #   glue::glue("(?<=(^| )){.reg_num_start}(?:{.reg_num_rest}| )*(?=( |$))")
  .reg_num <- 
    glue::glue("(?<![@#])\\b{.reg_num_start}(?:{.reg_num_rest}| )*s?\\b")
  
  return(.reg_num)
  
}

number_regex <- make_number_regex()

rm(make_number_regex)

################################################################################

make_ordinal_regex <- function(){
  
  .base_ord <- .ordinal <- 
    c("first", "second", "third", "fourth", "fifth", "sixth", "seventh", 
      "eighth", "ninth")
  
  .prefix <- 
    c("twent", "thirt", "fort", "fift", "sixt", "sevent", "eight", "ninet")
  
  .ordinal <- 
    c(.base_ord, "tenth", "eleventh", "twelfth", "thirteenth", "fourteenth", 
      "fifteenth", "sixteenth", "seventeenth", "eighteenth", "nineteenth", 
      paste0(rep(.prefix, each = 10), c("ieth", paste("y", .base_ord))), 
      "hundredth")
  
  .reg_ord <- 
    paste0("(?<![@#])\\b(", paste0(paste0('(?:', .ordinal, ')'), collapse='|'),
           ")\\b")
  
  return(.reg_ord)
  
}

ordinal_regex <- make_ordinal_regex()

rm(make_ordinal_regex)
