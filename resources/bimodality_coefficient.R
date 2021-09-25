# https://github.com/PascalKieslich/mousetrap/blob/master/R/bimodality.R

if(!"psych" %in% installed.packages()){install.packages("psych")}

bimodality_coefficient <- function(x, na.rm=FALSE){

  # Remove missing values, if desired
  if (na.rm) {
    x <- x[!is.na(x)]
  }

  n <- length(x)
  if (n == 0) {
  
    # The coefficient is not defined for
    # empty data
    return(NaN)

  } else {

    m3 <- psych::skew(x, type=2)
    m4 <- psych::kurtosi(x, type=2)

    # Calculate the coefficient based on
    # the above
    bc <- (m3^2 + 1) / (m4 + 3 * ((n - 1)^2 / ((n - 2) * (n - 3))))

    return(bc)

  }

}
