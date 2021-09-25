if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("D:/Documents/Uni/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

library(glue)
library(fs)
library(bit64)
library(tidyverse)
library(magrittr)
library(lubridate)
library(disk.frame)

source("./resources/disk.frame_extensions.R")
source("./resources/ggplot_theme_set.R")
source("./resources/plot_to_file.R")
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

workers <- 6
options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=1)
fst::threads_fst(nr_of_threads=workers)

################################################################################

data_dir <- path_wd("data")
temp_dir <- set_temp_dir(path(data_dir, "tmp"))

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
  mutate(
    across(status_topic_group, str_replace_all, 
           c("^BLM.*"="struktureller Rassismus"))
  )

################################################################################
################################################################################
################################################################################

roll_window_stats %>% 
  group_by(convers_candidate, date_end) %>% 
  mutate(window_status_share = 
           window_status_count/sum(window_status_count)) %>% 
  ungroup() %>% 
  filter(status_topic_group != "anderes Thema") %>%
  slice_sample(prop=1) %>% 
  ggplot(aes(x=date_end, y=window_status_share, group=convers_candidate,
             color=convers_candidate)) +
  geom_line() + 
  # geom_smooth(se=FALSE) +
  geom_vline(xintercept=election_day, color="#00AD80", linetype=2) +
  scale_color_viridis_d(begin=0.2, end=0.8, option="magma", 
                        labels=str_escape_tex) +
  scale_fill_viridis_d(begin=0.2, end=0.8, option="magma", 
                       labels=str_escape_tex) +
  scale_y_continuous(
    labels=partial(scales::percent, accuracy=1, suffix="\\%")
  ) +
  facet_wrap(
    facets=vars(str_escape_tex(status_topic_group)), nrow=1,
    # scales="free_y"
  ) +
  guides(
    color=guide_legend(title.position="top", title.hjust=0.5),
    fill="none"
  ) +
  labs(
    x="Datum", y="Anteil an allen\nAntworten des Tages", 
    color="Kandidat"
  ) +
  theme(
    panel.grid.minor.x=element_blank(),
    plot.margin=margin(0, 0, 0, 1, "pt")
  )

plot_to_file(
  last_plot(), "./figures/candidate_daily_status_topic_group_share.pdf",
  .width_rel=1, .height_rel=0.3
)

################################################################################

roll_window_stats %>% 
  group_by(convers_candidate, date_end) %>% 
  mutate(window_status_share = 
           window_status_count/sum(window_status_count)) %>% 
  ungroup() %>% 
  filter(window_status_count >= 50) %>%
  filter(status_topic_group != "anderes Thema") %>%
  select(convers_candidate, status_topic_group, date_end,
         matches("^window_pol_score_com_mean_cut_2_(divi|mean)")) %>% 
  pivot_longer(-c(1:3)) %>% 
  slice_sample(prop=1) %>% 
  mutate(across(
    name, str_replace_all, 
    c(".*divi$" = "Divisiveness", c(".*mean$" = "Mittelwert"))
  )) %>% 
  ggplot(aes(x=date_end, y=value, group=convers_candidate,
             fill=convers_candidate, color=convers_candidate)) +
  geom_smooth(aes(fill=convers_candidate), 
              method="gam", formula=y~s(x, bs="tp"),
              method.args=list(family=mgcv::scat)) +
  # geom_point(aes(fill=convers_candidate), shape=21, color="white") +
  geom_vline(xintercept=election_day, color="#00AD80", linetype=2) +
  scale_color_viridis_d(begin=0.2, end=0.8, option="magma", 
                        labels=str_escape_tex) +
  scale_fill_viridis_d(begin=0.2, end=0.8, option="magma", 
                        labels=str_escape_tex) +
  facet_grid(rows=vars(str_escape_tex(name)),
             cols=vars(str_escape_tex(status_topic_group)),
             scales="free_y") +
  guides(
    color=guide_legend(title.position="top", title.hjust=0.5),
    fill="none"
  ) +
  labs(x="Datum", y="Beobachteter Wert", color="Kandidat")

################################################################################

roll_window_stats %>% 
  group_by(convers_candidate, status_topic_group) %>% 
  summarise(window_status_count = sum(window_status_count), .groups="drop") %>% 
  filter(status_topic_group != "anderes Thema") %>%
  ggplot(aes(x=fct_rev(status_topic_group), y=window_status_count)) +
  geom_col(aes(fill=status_topic_group)) +
  facet_wrap(vars(str_escape_tex(convers_candidate)), scales="free",
             ncol=2) +
  scale_y_continuous(
    labels=function(.b){prettyNum(.b, big.mark="\\\\,", scientific=FALSE)},
    expand=expansion(mult=c(0, 0.025))
  ) +
  scale_fill_viridis_d(begin=0.2, end=0.8, option="magma", 
                       labels=str_escape_tex) +
  labs(y="Anzahl von Antworten", fill="Thema") +
  guides(fill=guide_legend(title.position="top", title.hjust=0.5)) +
  coord_flip() +
  theme(
    axis.text.y=element_blank(),
    axis.ticks.y=element_blank(),
    axis.title.y=element_blank(),
    panel.grid.major.y=element_blank(),
    panel.grid.minor.y=element_blank(),
  )

plot_to_file(
  last_plot(), "./figures/candidate_total_status_topic_group_count.pdf",
  .width_rel=1, .height_rel=0.3
)

################################################################################
################################################################################
################################################################################

roll_window_stats %>% 
  filter(window_status_count >= 50) %>% 
  filter(status_topic_group != "anderes Thema") %>% 
  transmute(
    convers_candidate, date_end,
    status_topic_group = 
      recode(status_topic_group,
             "struktureller Rassismus"="struktureller\nRassismus",
             "COVID-19-Pandemie"="COVID-19-\nPandemie"),
    window_pol_score_com_mean_cut_2_table = 
      window_pol_score_com_mean_cut_2_json_table %>% 
      map(jsonlite::fromJSON) %>% 
      map(as_tibble)
  ) %>% 
  unnest(window_pol_score_com_mean_cut_2_table) %>% 
  mutate(across(c(value), as.double)) %>% 
  group_by(convers_candidate, status_topic_group, date_end) %>% 
  mutate(share = count/sum(count)) %>% 
  ungroup() %>% 
  ggplot(aes(x=date_end, y=share, group=value, fill=value, color=value)) +
  # geom_rug(stat = "summary", fun.y = "mean") +
  geom_rug(
    aes(y=share, color=value), inherit.aes=FALSE,
    size=1, sides="l",
    data=function(.dat){
      summarise(group_by(.dat, convers_candidate, status_topic_group, value),
                share=mean(share), .groups="drop")
    }
  ) +
  stat_smooth(geom="line", method="loess",
              formula=y ~ x, span=0.5) +
  geom_point(
    size=0.2
    # shape=21, color="white"
  ) +
  stat_smooth(geom="ribbon", alpha=0.3, size=0, method="loess",
              formula=y ~ x, span=0.5) +
  geom_vline(xintercept=election_day, color="#00AD80", linetype=2) +
  scale_fill_viridis_c(option="magma", begin=0.2, end=0.8, 
                       breaks=c(-1, 0, 1), labels=round) +
  scale_color_viridis_c(option="magma", begin=0.2, end=0.8, 
                        breaks=c(-1, 0, 1)) +
  scale_y_continuous(labels=partial(scales::percent, suffix="\\%")) + 
  facet_grid(rows=vars(status_topic_group),
             cols=vars(str_escape_tex(convers_candidate))) +
  guides(
    fill=guide_colorbar(
      title.position="top", title.hjust=0.5, raster=FALSE, nbin=3, 
      barheight=unit(5, "pt"), barwidth=unit(50, "pt")
    ),
    color="none"
  ) +
  labs(x="Datum", y="Anteil an allen Tweets des Tages", fill="Sentimentwert") +
  theme(plot.margin=margin(0, 0, 0, 1, "pt"))

plot_to_file(
  last_plot(), "./figures/roll_window_pol_score_candidate_topic_group.pdf",
  .width_rel=1, .height_rel=0.7
)

roll_window_stats %>% 
  filter(window_days == filter_window_days, 
         max_dist_from_start == filter_max_dist_from_start) %>% 
  transmute(
    date_end, convers_candidate, status_topic_group, 
    window_pol_score_com_mean_cut_2_table = 
      window_pol_score_com_mean_cut_2_json_table %>% 
      map(jsonlite::fromJSON) %>% 
      map(as_tibble) %>% 
      map(mutate, across(everything(), as.numeric))
  ) %>% 
  unnest(window_pol_score_com_mean_cut_2_table, 
         names_sep="_") %>% 
  group_by(convers_candidate, status_topic_group,
           window_pol_score_com_mean_cut_2_table_value) %>% 
  summarise(across(window_pol_score_com_mean_cut_2_table_count, sum)) %>% 
  group_by(convers_candidate, status_topic_group) %>% 
  mutate(across(window_pol_score_com_mean_cut_2_table_count, ~.x/sum(.x))) %>% 
  ggplot(
    aes(x=window_pol_score_com_mean_cut_2_table_value,
        y=`/`(window_pol_score_com_mean_cut_2_table_count,
              sum(window_pol_score_com_mean_cut_2_table_count)),
        fill=window_pol_score_com_mean_cut_2_table_value)
  ) + 
  geom_col() +
  scale_fill_viridis_c(end=0.8, breaks=c(-1, 0, 1), labels=round) +
  scale_y_continuous(labels=partial(scales::percent, suffix="\\%")) + 
  guides(
    fill=guide_colorbar(
      title.position="top", title.hjust=0.5, raster=FALSE, nbin=3, 
      barheight=unit(5, "pt"), barwidth=unit(50, "pt")
    )
  ) +
  facet_grid(rows=vars(convers_candidate), cols=vars(status_topic_group),
             scales="free") +
  theme(axis.ticks.x=element_blank(),
        axis.text.x=element_blank(),
        panel.grid.major.x=element_blank(),
        panel.grid.minor.x=element_blank())

roll_window_stats %>% 
  filter(window_days == filter_window_days, 
         max_dist_from_start == filter_max_dist_from_start) %>% 
  mutate(across(matches("window_prsp_.+_share"), function(.x){
    case_when(window_status_prsp_count < 30 ~ NA_real_,
              TRUE ~ .x)
  })) %>% 
  filter(window_status_count > 50) %>% 
  filter(status_topic_group != "anderes Thema") %>%
  group_by(date_end, convers_candidate) %>% 
  ungroup() %>% 
  select(
    status_topic_group, date_end, convers_candidate,
    matches("^window_pol_score_com_mean_cut_2_(mean|divi)"),
  ) %>% 
  pivot_longer(-c(status_topic_group, date_end, convers_candidate),
               names_to="measure") %>% 
  group_by(status_topic_group, date_end, convers_candidate, measure) %>% 
  summarise(across(value, mean, .names="y"), .groups="drop") %>% 
  filter(status_topic_group != "anderes Thema") %>%
  slice_sample(prop=1) %>% 
  ggplot(aes(x=date_end, y=y, group=convers_candidate, 
             fill=convers_candidate)) +
  geom_point(shape=21, color="white") +
  stat_smooth(geom="ribbon", alpha=0.2, size=0, 
              method="gam", formula=y~s(x, bs="tp"), 
              method.args=list(select=FALSE)) +
  geom_vline(xintercept=election_day, linetype=2, color="#00AD80") +
  scale_fill_manual(
    values=c(viridis::magma(4, begin=0.2, end=0.8), "gray80")
  ) +
  facet_grid(vars(measure), vars(status_topic_group), scales="free_y")

################################################################################
################################################################################
################################################################################

roll_window_stats %>% 
  filter(window_status_count >= 40) %>%
  filter(status_topic_group != "anderes Thema") %>% 
  select(convers_candidate, status_topic_group, date_end,
         window_pol_score_com_mean_mean,
         matches("window_pol_score_com_mean_cut_2_(divi)")) %>% 
  pivot_longer(-c(convers_candidate, status_topic_group, date_end)) %>% 
  ggplot(aes(x=date_end, y=value, color=convers_candidate, 
             fill=convers_candidate)) +
  geom_line() +
  # geom_smooth(size=0) +
  # geom_point() +
  facet_grid(rows=vars(name), cols=vars(status_topic_group), scales="free_y") +
  scale_color_viridis_d(option="magma", begin=0.2, end=0.8) +
  scale_fill_viridis_d(option="magma", begin=0.2, end=0.8)
