if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("D:/Documents/uni/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

################################################################################

library(fs)
library(glue)
library(bit64)
library(magrittr)
library(tidyverse)
library(lubridate)
library(mgcv)

source("./resources/disk.frame_extensions.R")
source("./resources/plot_to_file.R")
source("./resources/ggplot_theme_set.R")
source("./resources/bimodality_coefficient.R")
source("./resources/divisiveness_coefficient.R")

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

workers <- 5

options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=1)
quanteda::quanteda_options(threads=workers)
fst::threads_fst(nr_of_threads=workers)

################################################################################

data_dir <- path_wd("data")
temp_dir <- set_temp_dir(path(data_dir, "tmp"))

################################################################################
################################################################################
################################################################################

election_day <- ymd("2020-11-03", tz="EST")
study_tscope <- interval(election_day-weeks(10), ymd("2021-01-06", tz="EST"))

################################################################################

filter_window_days <- 1
filter_max_dist_from_start <- Inf

################################################################################

convers_info <- 
  read_rds(path(data_dir, "02-convers_info.rds"))

roll_window_stats <-
  read_rds(path(data_dir, "05-roll_window_stats.rds")) %>% 
  filter(
    window_days == filter_window_days, 
    max_dist_from_start == filter_max_dist_from_start
  ) %>% 
  filter(
    time_length(election_day-date_end, "days") %% first(window_days) == 0
  ) %>% 
  group_by(convers_candidate, date_end) %>% 
  mutate(window_topic_status_share = 
           window_status_count/sum(window_status_count)) %>% 
  ungroup()

################################################################################
################################################################################
################################################################################

plot_concurvity <- function(.fit){
  
  .fit %>% 
    mgcv::concurvity(full=FALSE) %>% # names()
    chuck("worst") %>%
    as.data.frame() %>% 
    rownames_to_column() %>% 
    pivot_longer(-rowname, names_to="colname") %>% 
    mutate(across(matches("(row|col)name"), factor)) %>% 
    filter(as.integer(rowname) < as.integer(colname)) %>% 
    ggplot(aes(x=rowname, y=colname, fill=value)) +
    geom_bin2d(color="white", size=1) + 
    scale_x_discrete(position="top") +
    scale_fill_viridis_c(guide=guide_colorbar(barwidth=unit(100, "pt"))) +
    theme(axis.text.x=element_text(angle=90, hjust=0),
          panel.grid=element_blank())
  
}

################################################################################

make_pred_gam <- function(.data, .fit){
  
  .vars <- c("convers_candidate", "date_end_num_day", "status_topic_group")
  
  .data_grid_list <- 
    .data %>% 
    select(-1) %>%
    summarise(
      across(all_of(.vars), function(.x){
        if(is(.x, "numeric")){
          return(list(modelr::seq_range(.x, n=200)))
        }
        if(is(.x, "factor")){
          return(list(unique(.x)))
        }
      }),
      across(-all_of(.vars), function(.x){
        if(is(.x, "numeric")){
          return(list(mean(.x, na.rm=TRUE)))
        }
        if(is(.x, "factor")){
          return(list(fct_unify(names(table(.x))[1], .x)))
        }
      })
    ) %>% 
    as.list() %>% 
    map(unlist)
  
  .data_grid <- 
    expand_grid(!!!.data_grid_list) %>% 
    left_join(
      .data %>% 
        group_by(across(c(convers_candidate, status_topic_group))) %>% 
        summarise(across(date_end_num_day, list(min=min, max=max)), 
                  .groups="drop"),
      by=c("convers_candidate", "status_topic_group")
    ) %>% 
    filter(
      date_end_num_day > date_end_num_day_min,
      date_end_num_day < date_end_num_day_max
    ) %>% 
    select(-date_end_num_day_min, -date_end_num_day_max) %>% 
    mutate(
      date_end = 
        int_start(study_tscope) + ddays(date_end_num_day),
    )
  
  .pred_raw <- 
    mgcv:::predict.gam(.fit, newdata=.data_grid, se.fit=TRUE, type="link")
  
  .pred_conf <- 
    tibble(y = .pred_raw$fit, 
           ymin = .pred_raw$fit - qnorm(0.975) * .pred_raw$se.fit,
           ymax = .pred_raw$fit + qnorm(0.975) * .pred_raw$se.fit) %>% 
    mutate(across(everything(), .fit$family$linkinv))
  
  .plot_data <- bind_cols(.data_grid, .pred_conf)

}

################################################################################
################################################################################
################################################################################

roll_window_stats <- 
  roll_window_stats %>% 
  mutate(
    date_end_num_day =
      time_length(int_start(study_tscope) %--% date_end, unit="days"),
    date_end_week_day = 
      wday(date_end, label=FALSE),
  ) %>% 
  mutate(across(where(is.character)|where(is.logical), factor))

glimpse(roll_window_data)

################################################################################

roll_window_model_form_gam_list <- list(
    "Polarisiertheit"=  
      window_pol_score_com_mean_cut_2_divi ~
      convers_candidate * status_topic_group + 
      # s(date_end_week_day, bs="cc", k=7) +
      # s(date_end_num_day, by=convers_candidate) +
      # s(date_end_num_day, by=status_topic_group) +
      s(date_end_num_day, 
        by=interaction(convers_candidate, status_topic_group)) +
      1,
    "Durchschnitt"=  
      window_pol_score_com_mean_cut_2_mean ~
      convers_candidate * status_topic_group + 
      # s(date_end_week_day, bs="cc", k=7) +
      # s(date_end_num_day, by=convers_candidate) +
      # s(date_end_num_day, by=status_topic_group) +
      s(date_end_num_day, 
        by=interaction(convers_candidate, status_topic_group)) +
      1
)
  
################################################################################

roll_window_model_data <- 
  roll_window_stats %>% 
  filter(window_status_count >= 50) %>%
  filter(status_topic_group != "anderes Thema") %>%
  select(
    all_of(flatten_chr(map(roll_window_model_form_gam_list, all.vars))),
    date_end
  ) %>% 
  drop_na() %>% 
  filter(across(everything(), is.finite)) %>% 
  mutate(across(where(is.factor), fct_drop))

roll_window_model_data %>% 
  group_by()


################################################################################

roll_window_model_data %>% 
  select(where(is.numeric)) %>% 
  pivot_longer(everything(), names_to="var") %>% 
  ggplot(aes(x=value)) +
  geom_histogram(fill="grey80", size=0, bins=20) +
  geom_vline(
    aes(xintercept=value), linetype=2, color="grey20",
    data=~{summarise(group_by(.x, var), across(value, mean))}
  ) +
  facet_wrap(facets=vars(var), scales="free")

roll_window_model_data %>% 
  transmute(across(where(is.factor), as.character)) %>% 
  pivot_longer(everything(), names_to="var") %>% 
  ggplot(aes(x=value)) +
  geom_bar(fill="grey80") +
  facet_grid(rows=vars(var), scales="free", space="free", switch="y") +
  coord_flip() +
  theme(strip.placement="outside")

################################################################################
################################################################################
################################################################################

roll_window_model <- read_rds("./data/06-roll_window_fit_day.rds")
roll_window_model_gam <-
  roll_window_model_form_gam_list %>%
  enframe(value="form") %>%
  mutate(fit = map(form, function(.form){

    if(.form[[2]] == "window_topic_status_share"){
      .family <- mgcv::betar
    }else{
      .family <- mgcv::scat
    }

    gam(formula=.form,
        data=roll_window_model_data,
        family=.family,
        method="REML",
        # select=TRUE,
        knots=list(date_end_week_day = c(0.5,7.5)),
        control=mgcv::gam.control(trace=TRUE))

  })) %>%
  mutate(pred = map(fit, make_pred_gam, .data=roll_window_model_data))# %>%
  # write_rds("./data/06-roll_window_fit_day.rds")

beepr::beep(5)

map(pluck(roll_window_model_gam, "fit"), mgcv:::summary.gam)
map(pluck(roll_window_model_gam, "fit"), gratia::appraise)
map(pluck(roll_window_model_gam, "fit"), mgcv::gam.check)
map(pluck(roll_window_model_gam, "fit"), gratia:::draw.gam, parametric=FALSE, 
    ncol=4)
map(pluck(roll_window_model_gam, "fit"), plot_concurvity)
map(pluck(roll_window_model_gam, "fit"), mgcv::anova.gam)

################################################################################
################################################################################
################################################################################

roll_window_model_gam %>% 
  select(name, pred) %>% 
  unnest(pred) %>% 
  distinct() %>% 
  ggplot(aes(
    x=date_end, fill=convers_candidate, color=convers_candidate, y=y
  )) +
  geom_point(
    size=0.2,
    data=
      roll_window_model_data %>% 
      pivot_longer(starts_with("window_pol_score"), values_to="y") %>% 
      mutate(across(
        name, str_replace_all,
        c(".*mean$"="Durchschnitt", ".*divi$"="Polarisiertheit")
      ))
  ) +
  geom_line() +
  geom_ribbon(aes(ymin=ymin, ymax=ymax), alpha=0.3, size=0) +
  geom_vline(xintercept=election_day, color="#00AD80", linetype=2) +
  scale_color_viridis_d(option="magma", begin=0.2, end=0.8, 
                        labels=str_escape_tex) +
  scale_fill_viridis_d(option="magma", begin=0.2, end=0.8, 
                       labels=str_escape_tex) +
  facet_grid(rows=vars(fct_rev(name)), 
             cols=vars(status_topic_group),
             scales="free_y") +
  labs(x="Datum", color="Kandidat", fill="Kandidat", y="Erwartungswert") +
  guides(
    fill=guide_legend(title.position="top", title.hjust=0.5),
    color=guide_legend(title.position="top", title.hjust=0.5)
  ) +
  theme(
    plot.margin=unit(c(0, 0, 0, 0), "pt"),
    panel.grid=element_line(color="gray85"),
    panel.background=element_rect(fill="gray98", size=0),
    legend.title.align=0.5
  )

plot_to_file(
  last_plot(), "./figures/candidate_topic_election_pol_score_nonlin.pdf",
  .width_rel=1, .height_rel=0.6
)
