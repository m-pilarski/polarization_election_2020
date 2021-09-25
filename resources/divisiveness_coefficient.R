# https://github.com/TheRensselaerIDEA/COVID-masks-nlp/blob/master/analysis/
# plot_tweet_timeseries.R
divisiveness_coefficient <- function(x, na.rm=FALSE){
  ########################################################################################
  # INPUT:
  #   x : vector of VADER compound sentiment scores
  # OUTPUT:
  #   vector of divisiveness scores
  #
  # Divisiveness score ranges between -∞ and ∞
  # score = ∞  ==> absolute division of sentiments (ex: Bernoulli dist.)
  # score = 0  ==> sentiments are perfectly spread out (ex: uniform dist.)
  # score = -∞ ==> absolute consensus of sentiments (ex: Laplace dist. with small scale)
  # For more information on the Sarle's bimodality coefficient, check out the article:
  # ---
  # Pfister R, Schwarz KA, Janczyk M, Dale R, Freeman JB. Good things peak in pairs: a
  # note on the bimodality coefficient. Front Psychol. 2013;4:700. Published 2013 Oct 2.
  # doi:10.3389/fpsyg.2013.00700
  # ---
  ########################################################################################
  
  if(na.rm){x <- x[!is.na(x)]}
  if(n_distinct(x) == 1){return(-Inf)}
  
  n <- length(x)
  
  if (n > 3) {
    skew.mean <- moments::skewness(x)
    kurt.mean <- moments::kurtosis(x)
    skew.var <- 6 * n * ( n - 1) / ((n - 2) * (n + 1) * (n + 2))
    skew.squared.var <- skew.var^2 + 2 * skew.var * skew.mean^2
    kurt.var <- 4 * skew.var * (n^2 - 1) / ((n - 3) * (n + 5))
    BC.mean <- (skew.mean^2 + 1) / kurt.mean # compute Sarle's BC
    BC.var <- skew.squared.var / kurt.mean^2 + kurt.var * (skew.mean^2 + 1)^2 / kurt.mean^4
    phi <- 5/9
    Z <- abs(BC.mean - phi) / sqrt(BC.var)
    w <- pracma::erf(Z / sqrt(2))
    BCc <- w * BC.mean + (1 - w) * phi
    return(pracma::logit(BCc) - pracma::logit(phi))
  } else {
    return(0)
  }
}
