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

workers <- 5

options(future.globals.maxSize=Inf)
disk.frame::setup_disk.frame(workers=1)
quanteda::quanteda_options(threads=workers)
fst::threads_fst(nr_of_threads=workers)

################################################################################

election_day <- ymd("2020-11-03", tz="EST")
study_tscope <- interval(election_day-weeks(10), ymd("2021-01-06", tz="EST"))

################################################################################
################################################################################
################################################################################

convers_info <- read_rds("./data/02-convers_info.rds")

status_id_created_at <- 
  disk.frame("./data/02-convers_full_raw.df/") %>% 
  select(status_id, created_at) %>% 
  collect()

convers_id_status_pol_score_com_mean <- 
  disk.frame(path(data_dir, "04-convers_full_status_pol_score.df")) %>% 
  select(convers_id, status_id, status_pol_score_com_mean) %>% 
  collect() %>% 
  semi_join(convers_info, by="convers_id") %>% 
  semi_join(
    read_rds(path(data_dir, "02-status_id_has_term_count_and_vocab.rds")),
    by="status_id"
  ) %>% 
  left_join(select(convers_info, matches("^convers_(id|candidate)$")),
            by="convers_id") %>% 
  mutate(status_pol_score_com_mean_cut_2 = 
           status_pol_score_com_mean %>% 
           abs() %>% 
           cut.default(c(0, 0.05, 1), include.lowest=TRUE, right=TRUE) %>% 
           as.numeric() %>%
           `-`(1) %>%
           `/`(max(., na.rm=TRUE)) %>%
           `*`(sign(status_pol_score_com_mean))) %>% 
  mutate(status_pol_score_com_mean_cut_2_fct = 
           factor(status_pol_score_com_mean_cut_2))

################################################################################
################################################################################
################################################################################

candidate_total_pol_score_share <- 
  convers_id_status_pol_score_com_mean %>% 
  count(convers_candidate, status_pol_score_com_mean_cut_2,
        name="count") %>% 
  group_by(convers_candidate) %>% 
  mutate(share = count/sum(count)) %>% 
  ungroup() %>% 
  ggplot(aes(x=status_pol_score_com_mean_cut_2, y=share)) +
  geom_col(
    # aes(fill=status_pol_score_com_mean_cut_2)
    fill="gray80"
  ) + 
  geom_vline(
    aes(xintercept=mean), color="#00AD80", linetype=2,
    data=function(.data){
      summarise(
        group_by(.data, convers_candidate), 
        mean=weighted.mean(status_pol_score_com_mean_cut_2, share)
      )
    }
  ) +
  scale_y_continuous(labels=function(.b){scales::percent(.b, suffix="\\%")}) +
  scale_fill_viridis_c(option="magma", begin=0.2, end=0.8) +
  facet_wrap(vars(str_escape_tex(convers_candidate)), nrow=2, strip.position="right") +
  labs(x="Sentiment", y="Anteil") +
  guides(fill=guide_colorbar(
    title.position="top", title.hjust=0.5, raster=FALSE, nbin=3, 
    barheight=unit(5, "pt"), barwidth=unit(50, "pt")
  )) +
  theme(legend.position="none"); candidate_total_pol_score_share

plot_to_file(candidate_total_pol_score_share, 
             "./figures/candidate_total_pol_score_share.pdf",
             .width_rel=2/3, .height_rel=0.2)

################################################################################
################################################################################
################################################################################

candidate_daily_pol_score_mean <- 
  convers_id_status_pol_score_com_mean %>% 
  select(status_id, convers_candidate, status_pol_score_com_mean_cut_2) %>% 
  left_join(status_id_created_at, by="status_id") %>% 
  group_by(convers_candidate, status_createt_at_date = date(created_at)) %>% 
  summarise(across(status_pol_score_com_mean_cut_2,
                   list(mean=mean)),
            .groups="drop") %>% 
  ggplot(aes(x=status_createt_at_date, 
             y=status_pol_score_com_mean_cut_2_mean,
             color=convers_candidate)) +
  geom_line() +
  geom_vline(xintercept=date(election_day), color="#00AD80", linetype=2) +
  scale_color_viridis_d(option="magma", begin=0.2, end=0.8,
                        labels=str_escape_tex) +
  labs(x="Datum", y="Durchschnittliches\nSentiment", color="Kandidat") +
  guides(color=guide_legend(title.position="top", title.hjust=0.5))

plot_to_file(candidate_daily_pol_score_mean, 
             "./figures/candidate_daily_pol_score_mean.pdf",
             .width_rel=0.5, .height_rel=0.4)

################################################################################
################################################################################
################################################################################

candidate_total_pol_score_share + candidate_daily_pol_score_mean +
  patchwork::plot_layout(nrow=1, 
                         # widths=c(2/3, 1/3)
                         ) +
  theme(legend.position="bottom")

plot_to_file(last_plot(), 
             "./figures/candidate_pol_score_summary.pdf",
             .width_rel=1, .height_rel=0.3)
 