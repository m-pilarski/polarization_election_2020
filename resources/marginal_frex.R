marginal_frex <- function(.model, .n=10, .w=0.5){
  
  logbeta <- .model$beta$logbeta
  K <- .model$settings$dim$K
  vocab <- .model$vocab
  margbeta <- exp(logbeta[[1]])
  if (length(logbeta) > 1) {
    weights <- .model$settings$covariates$betaindex
    tab <- table(weights)
    weights <- tab/sum(tab)
    margbeta <- margbeta * weights[1]
    for (i in 2:length(.model$beta$logbeta)) {
      margbeta <- margbeta + exp(.model$beta$logbeta[[i]]) * weights[i]
    }
  }
  
  margbeta <- log(margbeta)
  wordcounts <- .model$settings$dim$wcounts$x

  logbeta <- margbeta
  
  excl <- t(t(logbeta) - stm:::col.lse(logbeta))
  if (!is.null(wordcounts)) {
    excl <- stm:::safelog(sapply(1:ncol(excl), function(x){
      stm::js.estimate(exp(excl[,x]), wordcounts[x])
    }))
  }
  freqscore <- apply(logbeta, 1, data.table::frank)/ncol(logbeta)
  exclscore <- apply(excl, 1, data.table::frank)/ncol(logbeta)
  
  frex <- 1/(.w/freqscore+(1-.w)/exclscore)
  
  frex %>% 
    `rownames<-`(1:nrow(.)) %>% 
    `colnames<-`(1:ncol(.)) %>% 
    as.data.frame() %>% 
    rownames_to_column(var="term_id") %>% 
    pivot_longer(-term_id, names_to="topic_id", values_to="frex_score") %>% 
    mutate(across(ends_with("_id"), as.integer)) %>% 
    arrange(topic_id, -frex_score) %>% 
    group_by(topic_id) %>% 
    slice_head(n=.n) %>% 
    ungroup() %>% 
    mutate(term = map_chr(term_id, ~{vocab[.x]}))
  
}


