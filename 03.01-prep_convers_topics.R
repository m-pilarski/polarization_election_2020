if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("I:/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

################################################################################

library(fs)
library(glue)
library(disk.frame)
library(Matrix)
library(tidyverse)
library(magrittr)
library(bit64)
library(furrr)
library(stm)
library(tidygraph)
library(ggraph)
library(ggiraph)

source("./resources/tidystm.R")
source("./resources/disk.frame_extensions.R")
source("./resources/plot_to_file.R")
source("./resources/ggplot_theme_set.R")

workers <- 5
options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=workers)
fst::threads_fst(1)
quanteda::quanteda_options(threads=1)

################################################################################

data_dir <- path_wd("data")
temp_dir <- dir_create(path(data_dir, "tmp"), recurse=FALSE)

################################################################################

ipolate <- function(.surface, .x, .y, .k=NA, .res=200){
  
  s <- mgcv::s
  
  .surface <- select(.surface, {{.x}}, {{.y}})
  .colnames_orig <- colnames(.surface)
  .surface <- set_colnames(.surface, c("x", "y"))
  
  .surface_mod <- mgcv::gam(y ~ te(x, k=.k), data=.surface)
  
  .surface_smooth <-
    tibble(x=seq(min(.surface$x), max(.surface$x), length.out=.res)) %>% 
    modelr::add_predictions(.surface_mod, var="y") %>%
    select(x, y) %>%
    set_colnames(.colnames_orig)
 
  return(.surface_smooth)
  
}

################################################################################
################################################################################
################################################################################

convers_stm_form <- 
  list(prevalence=~s(created_at_num)*screen_name,
       content=~screen_name)

convers_stm_data <- 
  read_rds("./data/02-convers_stm_data.rds") %>% 
  modify_in("meta", function(.df){
    select(.df, unique(flatten_chr(map(convers_stm_form, all.vars))))
  })

################################################################################

convers_stm_glance <- read_rds("./data/03-convers_stm_glance.rds")
convers_stm_glance <-
  stm(documents=convers_stm_data$documents,
      vocab=convers_stm_data$vocab,
      data=convers_stm_data$meta,
      prevalence=convers_stm_form$prevalence,
      content=convers_stm_form$content,
      reportevery=1,
      K=0,
      sigma.prior=0,
      emtol=1e-05) %>%
  write_rds("./data/03-convers_stm_glance3.rds")

convers_stm_chull_K <- ncol(chuck(convers_stm_glance, "theta"))

################################################################################

convers_stm_search <-
  expand_grid(
    K = seq(from=10, to=110, by=10) %>% c(convers_stm_chull_K),
    sigma.prior = 0,
    run = seq_len(7),
  ) %>%
  distinct() %>% 
  arrange(run, abs(K-convers_stm_chull_K), sigma.prior) %>%
  mutate(search_fit = map(transpose(.), function(.par){

    .get_new <- FALSE 
    
    .search_fit_file_name <- 
      glue_data(.par, "search_fit_K={K}_sigma={sigma.prior}_run={run}.rds")
    .search_fit_path <- 
      path(data_dir, "03-convers_stm_search", .search_fit_file_name)
    
    .search_fit <- NULL
      
    if(!file_exists(.search_fit_path) & isTRUE(.get_new)){
      
      tryCatch({
        
        message(crayon::bold(crayon::bgWhite(
          str_c(str_c(names(.par), "=", as.character(.par)), collapse=", ")
        )))
        
        set.seed(pluck(.par, "run"))
        
        .search_fit <- 
          searchK(documents=convers_stm_data$documents,
                  vocab=convers_stm_data$vocab,
                  data=convers_stm_data$meta,
                  prevalence=convers_stm_form$prevalence,
                  content=convers_stm_form$content,
                  K=.par$K,
                  sigma.prior=.par$sigma.prior,
                  interactions=TRUE,
                  emtol=1e-05,
                  max.em.its=150,
                  reportevery=5)
        
        
      }, error=function(..error){
        
        message(..error$message)
        .search_fit <<- simpleError(..error$message)
        
      }, interrupt=function(...){
        
        stop("Interrupted by the user")
        
      })
      
      dir_create(path_dir(.search_fit_path), recurse=FALSE)
      write_rds(.search_fit, .search_fit_path)
      rm(.search_fit); gc()
      beepr::beep()
      
      .search_fit <- NULL
      
    }else if(file_exists(.search_fit_path)){
      
      .search_fit <- read_rds(.search_fit_path)
      
    }else{
      
      .search_fit <- NULL
      
    }

    return(.search_fit)

  }))

################################################################################

convers_stm_search_is_select <- 
  tribble(
    ~K,  ~sigma.prior,
    30,  0,
    78,  0,
    60,  0,
  )

################################################################################

convers_stm_search_plot_data <-
  convers_stm_search %>%
  mutate(across(search_fit, map, function(.search_fit){
    .search_fit %>%
      pluck("results", .default=tibble()) %>%
      select(-matches("^K$")) %>%
      mutate_if(is.list, unlist)
  })) %>%
  unnest(search_fit) %>%
  pivot_longer(cols=c(-K, -sigma.prior), names_to="measure") %>%
  filter(measure == "heldout")

convers_stm_search_plot <- 
  convers_stm_search_plot_data %>%
  group_by(K, sigma.prior) %>% 
  filter(K>=20) %>%
  summarise(value = mean(value),
            .groups="drop") %>%
  left_join(mutate(convers_stm_search_is_select, is_select = TRUE),
            by=c("K", "sigma.prior")) %>% 
  mutate(is_select = replace_na(is_select, FALSE)) %>% 
  mutate(is_select = 
           factor(is_select, levels=c(TRUE, FALSE), 
                  labels=c("nähere Betrachtung", "Ausschluss"))) %>% 
  ggplot(aes(x=K, y=value)) + 
  geom_line(alpha=0.2) +
  geom_point(size=1.5, shape=21, alpha=0.2, stroke=0.5, color="black", 
             fill="white", 
             data=filter(convers_stm_search_plot_data, K>=20)) +
  geom_point(aes(fill=is_select, color=is_select), shape=21, size=1.5,
             stroke=0.5) +
  scale_fill_viridis_d(option="magma", begin=0.4, end=0.6) + 
  scale_color_viridis_d(option="magma", begin=0.4, end=0.6) + 
  guides(fill=guide_legend(title.position="top", title.hjust=0.5,
                           override.aes=list(size=2, stroke=0)),
         color="none") +
  labs(fill="Auswahl", x="$K$", y="Heldout Likelihood") +
  theme(legend.justification="center")
  
if(interactive()){
  print(convers_stm_search_plot); Sys.sleep(0.5)
  plot_to_file(convers_stm_search_plot, .width_rel=0.5, .height_rel=0.4,
               "./figures/convers_stm_search_plot.pdf")
}

################################################################################
################################################################################
################################################################################

convers_stm_select <- 
  convers_stm_search_is_select %>%
  mutate(select_fit = map(transpose(.), function(.par){
    
    .get_new <- FALSE
    
    .select_fit_file_name <- 
      glue_data(.par, "select_fit_K={K}_sigma={sigma.prior}.rds")
    .select_fit_path <- 
      path(data_dir, "03-convers_stm_select", .select_fit_file_name)
    
    .select_fit <- NULL
    
    if(!file_exists(.select_fit_path) & isTRUE(.get_new)){
      
      tryCatch({
        
        message(crayon::bold(crayon::bgWhite(
          str_c(str_c(names(.par), "=", as.character(.par)), collapse=", ")
        )))
        
        .select_fit <- 
          stm(documents=convers_stm_data$documents,
              vocab=convers_stm_data$vocab,
              data=convers_stm_data$meta,
              prevalence=convers_stm_form$prevalence,
              content=convers_stm_form$content,
              K=.par$K,
              sigma.prior=.par$sigma.prior,
              interactions=TRUE,
              emtol=1e-06,
              max.em.its=150)
        
      }, error=function(..error){
        message(..error$message)
        .select_fit <<- simpleError(..error$message)
      }, interrupt=function(...){
        stop("Interrupted by the user")
      })
      
      dir_create(path_dir(.select_fit_path), recurse=FALSE)
      write_rds(.select_fit, .select_fit_path)
      rm(.select_fit); gc()
      beepr::beep()
      
      return(NULL)
      
    }else if(file_exists(.select_fit_path)){
      
      return(read_rds(.select_fit_path))
      
    }else{
      
      return(NULL)
      
    }
    
  })) %>% 
  mutate(select_fit = set_names(select_fit, glue("K{K}_S{sigma.prior}")))

################################################################################

names(convers_stm_select$select_fit)

convers_stm_best <- 
  convers_stm_select$select_fit$K78_S0 %>% 
  write_rds(path(data_dir, "03-convers_stm_best.rds"))

