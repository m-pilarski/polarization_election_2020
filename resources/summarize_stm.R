summarize_stm <- function(.stm_fit, .stm_data, ...){
  
  .topic_shares <- 
    .stm_fit %>% 
    pluck("theta") %>% 
    set_colnames(1:ncol(.)) %>% 
    colMeans() %>% 
    enframe(name="topic_id", value="topic_share") %>% 
    mutate(across(topic_id, as.integer))
  
  .topic_sem_cohs <- 
    semanticCoherence(.stm_fit, .stm_data$documents) %>% 
    set_names(1:length(.)) %>% 
    enframe(name="topic_id", value="topic_sem_coh") %>% 
    mutate(across(topic_id, as.integer))
  
  .topic_terms_raw <- labelTopics(.stm_fit, n=100)
  
  .topic_terms_main <-
    .topic_terms_raw %>%
    pluck("topics") %>%
    set_rownames(.topic_terms_raw$topicnums) %>%
    set_colnames(1:ncol(.)) %>%
    as_tibble(rownames="topic_id") %>%
    pivot_longer(-topic_id, names_to="prob_rank", values_to="term") %>%
    mutate(across(c(topic_id, prob_rank), as.integer))
  
  # .topic_terms_marginal <- sageLabels(.stm_fit, n=100)
  # 
  # .topic_terms_main <- 
  #   .topic_terms_marginal %>% 
  #   pluck("marginal", "frex") %>% 
  #   apply(1, function(..row){
  #     tibble(prob_rank=seq_along(..row), term=..row)
  #   }) %>% 
  #   as_tibble_col("prob_terms") %>% 
  #   rowid_to_column("topic_id") %>% 
  #   unnest(prob_terms)
  
  # see stm:::report()
  .topic_terms_main_alt <-
    .stm_fit %>%
    pluck("beta", "kappa", "params") %>% 
    `[`(1:ncol(.stm_fit$theta)) %>% 
    map_dfr(function(..params){
      ..windex <- order(..params, decreasing=TRUE)[1:100]
      ..windex <- ..windex[..params[..windex] > 0]
      ..term <- .stm_fit$vocab[..windex]
      tibble(term = ..term, prob_rank = seq_along(..term))
    }, .id="topic_id") %>%
    mutate(across(c(topic_id, prob_rank), as.integer)) %>% 
    select(topic_id, prob_rank, term)

  .topic_terms_covar <-
    .topic_terms_raw %>%
    pluck("covariate") %>%
    set_colnames(1:ncol(.)) %>%
    as_tibble(rownames="screen_name") %>%
    pivot_longer(-screen_name, names_to="prob_rank", values_to="term") %>%
    mutate(across(prob_rank, as.integer))
  
  .topic_terms_inter <- 
    .topic_terms_raw %>% 
    pluck("interaction") %>%
    set_rownames(1:nrow(.)) %>%
    set_colnames(1:ncol(.)) %>%
    as_tibble(rownames="row_id") %>%
    mutate(screen_name = rep(row.names(.topic_terms_raw$covariate), n()/4),
           topic_id = 1 + as.integer(row_id) %/% 4) %>%
    select(-row_id) %>%
    pivot_longer(-c(screen_name, topic_id), names_to="prob_rank",
                 values_to="term") %>%
    mutate(across(c(topic_id, prob_rank), as.integer))
  
  list(topic_shares = .topic_shares,
       topic_sem_cohs = .topic_sem_cohs,
       topic_terms_main = .topic_terms_main, 
       topic_terms_main_alt = .topic_terms_main_alt, 
       topic_terms_covar = .topic_terms_covar, 
       topic_terms_inter = .topic_terms_inter)
  
}
