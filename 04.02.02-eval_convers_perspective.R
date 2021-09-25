if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("I:/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

################################################################################

devtools::install_github("favstats/peRspective", quiet=TRUE)

library(fs)
library(bit64)
library(disk.frame)
library(tidyverse)
library(lubridate)
library(mgcv)

source("./resources/disk.frame_extensions.R")
source("./resources/ggplot_theme_set.R")
source("./resources/plot_to_file.R")

################################################################################

data_dir <- path_wd("data")
temp_dir <- dir_create(path(data_dir, "tmp"), recurse=FALSE)

################################################################################

workers <- 1

options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=1)
quanteda::quanteda_options(threads=workers)
fst::threads_fst(1L)

################################################################################

election_day <- ymd("2020-11-03", tz="EST")

prsp_attr_nytimes <- 
  c("attack_on_author", "attack_on_commenter", "incoherent", "inflammatory", 
    "likely_to_reject", "obscene", "spam", "unsubstantial")

################################################################################

convers_stm_best_topic_props <- 
  bind_cols(
    stm::make.dt(read_rds(path(data_dir, "03-convers_stm_best.rds"))),
    transmute(read_rds(path(data_dir, "02-convers_stm_data.rds"))$meta,
              across(convers_id, bit64::as.integer64))
  ) %>% 
  select(-docnum) %>% 
  pivot_longer(-convers_id, names_to="topic_id", values_to="topic_prop") %>% 
  mutate(across(topic_id, function(.id){
    as.integer(str_extract(.id, "(?<=^Topic)\\d+$"))
  })) %>% 
  (function(.topic_props){
    .topic_props %>% 
      group_by(topic_id) %>% 
      summarise(topic_prop = mean(topic_prop), .groups="drop") %>% 
      transmute(
        topic_id = as.integer(fct_reorder(as.character(topic_id), topic_prop))
      ) %>% 
      left_join(.topic_props, by="topic_id")
  }) %>% 
  pivot_wider(names_from="topic_id", values_from="topic_prop",
              names_glue="topic_{.name}")

reset_workers_df(.change_workers=workers)

status_prsp_score <- 
  collect(disk.frame(path(data_dir, "04-convers_sample_prsp.df"))) %>% 
  reset_workers_df(.change_workers=1) %>% 
  left_join(read_rds(path(data_dir, "05-convers_status_id_created_at.rds")), 
            by="status_id") %>% 
  left_join(select(read_rds(path(data_dir, "05-convers_info_extended.rds")),
                   convers_id, convers_candidate), 
            by="convers_id")

reset_workers_df(.change_workers=1)

status_prsp_score %>% 
  filter(status_created_at_day <= 135) %>%
  count(convers_candidate, convers_id, sort=TRUE) %T>% 
  print() %>% 
  ggplot(aes(x=n, fill=convers_candidate)) +
  geom_histogram(bins=30, position="stack") +
  geom_vline(xintercept=500) +
  scale_fill_viridis_d(option="magma", begin=0.2, end=0.8)

status_prsp_score %>% 
  filter(status_created_at_day <= 135) %>%
  count(convers_candidate, status_created_at_day, sort=TRUE) %T>% 
  print() %>% 
  ggplot(aes(x=n, fill=convers_candidate)) +
  geom_histogram(bins=30, position="stack") +
  geom_vline(xintercept=5000) +
  scale_fill_viridis_d(option="magma", begin=0.2, end=0.8)

################################################################################  

status_prsp_score %>% 
  filter(status_created_at_day <= 135) %>% 
  select(-error, -convers_id, -convers_candidate, -status_created_at_day) %>% 
  pivot_longer(-status_id, names_to="attr", values_to="score") %>% 
  drop_na() %>% 
  count(attr, is = score>0.8) %>% 
  ggplot(aes(x=0, y=n, fill=is)) +
  geom_col(position="stack") +
  facet_wrap(vars(attr)#, 
             # ncol=4
             ) +
  coord_polar(theta="y") +
  scale_fill_viridis_d(begin=0.4, end=0.6) +
  theme(legend.position="bottom",
        axis.title=element_blank(),
        axis.text=element_blank(),
        axis.ticks=element_blank(),
        panel.grid=element_blank())

status_prsp_score %>% 
  filter(status_created_at_day <= 135) %>% 
  select(-error, -convers_id, -status_id) %>% 
  pivot_longer(-c(status_created_at_day, convers_candidate),
               names_to="attr", values_to="score") %>% 
  filter(!attr %in% prsp_attr_nytimes) %>%
  drop_na() %>% 
  group_by(status_created_at_day, convers_candidate, attr) %>% 
  summarise(share_is = sum(score>0.85)/length(score),
            .groups="drop") %>% 
  ggplot(aes(x=status_created_at_day, y=share_is, 
             color=convers_candidate)) +
  geom_point(size=0.5) +
  geom_smooth(method="gam", level=0.95) +
  geom_vline(xintercept=70, color="#00AD80", linetype=2) +
  facet_wrap(vars(attr), scales="free_y", ncol=4) +
  scale_y_continuous(labels=scales::percent) +
  scale_color_viridis_d(option="magma", begin=0.2, end=0.8)
