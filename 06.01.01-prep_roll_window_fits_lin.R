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

make_pred <- function(.data, .fit){
  
  .group_vars <- 
    c("convers_candidate", "date_end_after_election", "status_topic_group")
  
  .data_grid_list <- 
    .data %>% 
    select(any_of(.group_vars)) %>%
    summarise(
      across(everything(), function(.x){
        if(is(.x, "numeric")){
          return(list(modelr::seq_range(.x, n=100)))
        }
        if(is(.x, "factor")){
          return(list(unique(.x)))
        }
      })
    ) %>% 
    as.list() %>% 
    map(unlist)
  
  .data_grid <- expand_grid(!!!.data_grid_list)
  
  .pred_raw <- 
    mgcv:::predict.gam(.fit, newdata=.data_grid, se.fit=TRUE, type="link")
  
  .pred_conf <- 
    tibble(y = .pred_raw$fit, 
           ymin = .pred_raw$fit - qnorm(0.975) * .pred_raw$se.fit,
           ymax = .pred_raw$fit + qnorm(0.975) * .pred_raw$se.fit) %>% 
    mutate(across(everything(), .fit$family$linkinv))
  
  .pred_data <- 
    bind_cols(.data_grid, .pred_conf) %>% 
    nest_join(.data, name="obs") %>% 
    mutate(has_nobs = factor(map_int(obs, nrow) > 5, levels=c(TRUE, FALSE),
                             labels=c("ja", "nein")))
  
  return(.pred_data)
  
}

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
  mutate(across(status_topic_group, str_replace_all, 
                c("^BLM.*"="struktureller Rassismus"))) %>% 
  mutate(date_end_after_election = date_end > election_day) %>% 
  mutate(across(where(is.character)|where(is.logical), factor))

################################################################################
################################################################################
################################################################################

roll_window_model_form_list <- list(
  "Polarisiertheit"=  
    window_pol_score_com_mean_cut_2_divi ~
    convers_candidate * status_topic_group * date_end_after_election,
  "Durchschnitt"=  
    window_pol_score_com_mean_cut_2_mean ~
    convers_candidate * status_topic_group * date_end_after_election
)

################################################################################
################################################################################
################################################################################

roll_window_model_data <- 
  roll_window_stats %>% 
  filter(window_status_count >= 50) %>%
  filter(status_topic_group != "anderes Thema") %>%
  select(all_of(flatten_chr(map(roll_window_model_form_list, all.vars))),
         date_end) %>% 
  drop_na() %>% 
  filter(across(everything(), is.finite)) %>% 
  mutate(across(where(is.factor), fct_drop)) %>% 
  (function(.data){
    
    .group_weights <- 
      .data %>% 
      count(
        convers_candidate, status_topic_group, date_end_after_election, 
        name="count"
      ) %>% 
      mutate(
        weight = (sum(count)/count)/mean(sum(count)/count),
      ) %>% 
      select(-count)
    
    .data_mod <- left_join(
      .data, .group_weights,
      by=c("convers_candidate", "status_topic_group", "date_end_after_election")
    )
    
    return(.data_mod)
     
  })

################################################################################
################################################################################
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

# roll_window_model <- read_rds("./data/06-roll_window_fit_period.rds")
roll_window_model <-
  roll_window_model_form_list %>%
  enframe(value="form") %>%
  mutate(fit = map(form, function(.form){

    gam(formula=.form,
        data=roll_window_model_data,
        # weights=roll_window_model_data$weight,
        family=mgcv::scat,
        method="REML",
        select=FALSE,
        control=mgcv::gam.control(trace=TRUE))

  })) %>%
  mutate(pred = map(fit, make_pred, .data=roll_window_model_data))# %>%
  # write_rds("./data/06-roll_window_fit_period.rds")

################################################################################
################################################################################
################################################################################

roll_window_model %>% 
  pluck("fit") %>% 
  map(mgcv:::summary.gam)

roll_window_model %>%
  pluck("fit") %>%
  map(gratia::qq_plot)

roll_window_model %>% 
  pluck("fit") %>% 
  map(mgcv:::anova.gam)

roll_window_model %>% 
  pluck("fit") %>% 
  map(mgcv:::anova.gam)

################################################################################
################################################################################
################################################################################

plot_data <- 
  roll_window_model %>% 
  select(name, pred) %>% 
  unnest(pred) %>% 
  distinct() %>% 
  mutate(
    x = `+`(as.numeric(convers_candidate),
            (as.numeric(date_end_after_election)-1.5)*0.5)
  ) %>% 
  nest_join(
    roll_window_model_data %>% 
      pivot_longer(
        matches("^window_pol_score_com_mean_cut_2_(divi|mean)$"),
        names_to="name", values_to="y"           
      ) %>% 
      mutate(
        name = str_replace_all(
          name,
          c(".*divi$"="Polarisiertheit", ".*mean$"="Durchschnitt")
        )
      ), 
    by=c("convers_candidate", "status_topic_group", "date_end_after_election"), 
    name="roll_window_model_data"
  ) %>% 
  mutate(
    roll_window_model_data =
      map2(roll_window_model_data, x, function(.df, .x){
        mutate(group_by(.df, name), 
               x = sample(seq(.x-0.25, .x+0.25, length.out=n())))
      })
  ) %>% 
  mutate(x = map(x, `+`, c(-0.2, 0.2))) %>% 
  rowid_to_column() %>% 
  unnest(x)

# view(unnest(select(plot_data_new, -x, -y, -name), roll_window_model_data))

plot_data %>% 
  ggplot(aes(
    x=x, y=y, 
    group=rowid, color=convers_candidate, fill=convers_candidate
  )) +
  # geom_point(s
  #   size=0.5, alpha=0.3, shape=21, color="transparent",
  #   data=function(.df){
  #     unnest(select(.df, -x, -y, -name), roll_window_model_data)
  #   }
  # ) +
  geom_ribbon(aes(ymin=ymin, ymax=ymax), alpha=0.5, size=0) +
  geom_line(size=1) +
  geom_line(
    aes(group=convers_candidate),
    size=1, linetype="dotted", # alpha=0.3,
    data=function(.dat){
      summarise(group_by(.dat, rowid),
                across(x, mean),
                across(-c(x, where(is.list)), first))
    }
  ) +
  scale_color_viridis_d(option="magma", begin=0.2, end=0.8,
                        labels=str_escape_tex) +
  scale_fill_viridis_d(option="magma", begin=0.2, end=0.8,
                       labels=str_escape_tex) +
  scale_x_continuous(
    breaks=unlist(map(1:4, `+`, c(-0.2, 0.2))),
    labels=rep(c("vor", "nach"), 4)
  ) +
  facet_grid(rows=vars(fct_rev(name)),
             cols=vars(status_topic_group),
             scales="free_y") +
  labs(x="Vor oder nach der Wahl", y="Erwartungswert", 
       color="Kandidat", fill="Kandidat") +
  guides(
    fill=guide_legend(title.position="top", title.hjust=0.5),
    color=guide_legend(title.position="top", title.hjust=0.5),
  ) +
  theme(
    plot.margin=unit(c(0, 0, 0, 0), "pt"),
    panel.grid=element_line(color="gray85"),
    panel.grid.minor.x=element_blank(),
    panel.background=element_rect(fill="gray98", size=0),
    axis.text.x=element_text(angle=45, hjust=1),
    legend.title.align=0.5
  )

plot_to_file(
  last_plot(), "./figures/candidate_topic_election_pol_score.pdf",
  .width_rel=1, .height_rel=0.6
)

# roll_window_model %>% 
#   mutate(fit = set_names(fit, name)) %>% 
#   pull(fit) %>% 
#   map(broom:::tidy.gam, parametric=TRUE) %>% 
#   bind_rows(.id="name") %>% 
#   mutate(p.star = case_when(
#     p.value <= 0.001 ~ "***", p.value <= 0.01 ~ "**", 
#     p.value <= 0.05 ~ "*", TRUE ~ ""
#   )) %>% 
#   select(name, term, estimate, se=std.error, p.star) %>% 
#   mutate(
#     across(term, str_escape_tex),
#     across(c(estimate, se), prettyNum, digits=3),
#     across(term, str_replace_all, "(^|:).*?(?=[A-Z])", " X "),
#     across(term, str_remove, "^ X ")
#   ) %>% 
#   mutate(
#     se = glue("([se])\\textsuperscript{[p.star]}", 
#               .open="[", .close="]")
#   ) %>% 
#   select(-p.star) %>% 
#   split(`$`(., name)) %>% 
#   bind_cols(.name_repair=janitor::make_clean_names) %>% 
#   select(-term_2) %>% 
#   rename_with(str_escape_tex) %>% 
#   select(-starts_with("name")) %>% 
#   kableExtra::kable(
#     format="latex", 
#     escape=FALSE, 
#     align=c("l", rep("r", 8)),
#   ) %>% 
#   kableExtra::kable_styling(
#     bootstrap_options="striped",
#     latex_options=c("striped"), 
#     font_size=8,
#   ) %>% 
#   kableExtra::column_spec(2:9, width="2cm") %>% 
#   str_remove_all("(?<=\\n)\\\\\\w*rule.*?\\n") %>% 
#   write_file("./tables/regression.tex")

roll_window_model %>% 
  mutate(fit = set_names(fit, name)) %>%
  pull(fit) %>% 
  stargazer::stargazer(
    single.row=TRUE,
    font.size="tiny",
    label="tab:par_regression"
  ) %>% 
  str_remove_all("(convers\\\\_candidate|status\\\\_topic\\\\_group)") %>% 
  str_replace_all("date\\\\_end\\\\_after\\\\_election", "danach") %>% 
  str_remove_all("@\\{\\\\extracolsep\\{5pt\\}\\}") %>% 
  write_lines("./tables/regg.tex")

################################################################################
################################################################################
################################################################################

roll_window_stats %>% 
  filter(status_topic_group != "anderes Thema") %>%
  mutate(across(date_end_after_election, fct_rev)) %>% 
  mutate(in_model_data = fct_rev(factor(window_status_count >= 50))) %>% 
  count(convers_candidate, status_topic_group, 
        date_end_after_election, in_model_data, 
        name="count") %>% 
  group_by(convers_candidate, date_end_after_election) %>% 
  mutate(share = count/sum(count),
         ymax = cumsum(share),
         ymin = lag(ymax, default=0)) %>% 
  ungroup() %>% 
  mutate(xmin = as.numeric(date_end_after_election)-0.45,
         xmax = as.numeric(date_end_after_election)+0.45) %>% #summary()
  ggplot(
    aes(
      xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax,
      fill=status_topic_group, alpha=in_model_data
    )
  ) +
  geom_rect() + 
  geom_text(
    aes(
      x=(xmin+xmax)/2, y=(ymin+ymax)/2,
      label=if_else(in_model_data == "TRUE", as.character(count), "")
    ), 
    inherit.aes=FALSE, 
    family="Franziska Pro", color="white", size=2.25
  ) + 
  geom_text(
    aes(x=x, y=y, label=if_else(convers_candidate=="JoeBiden", label, "")),
    inherit.aes=FALSE, 
    data=function(.df){
      expand_grid(
        tibble(x=c(-0.1, 3), y=c(0.5, 0.5), label=c("nach", "vor der Wahl")),
        distinct(.df, convers_candidate)
      )
    },
    family="Franziska Pro", size=2.25
  ) +
  scale_alpha_manual(values=c("TRUE"=1, "FALSE"=0)) + 
  scale_fill_viridis_d(option="magma", begin=0.2, end=0.8) + 
  facet_wrap(vars(str_escape_tex(convers_candidate)), nrow=1) +
  coord_polar(theta="y") +
  xlim(c(-1.75, 3)) + 
  guides(alpha="none",
         fill=guide_legend(title.position="top", title.hjust=0.5)) +
  labs(fill="Themenkomplex") +
  theme(axis.title=element_blank(),
        axis.text=element_blank(),
        axis.ticks=element_blank(),
        axis.line=element_blank(),
        panel.grid.minor.x=element_blank(),
        panel.grid.major.x=element_blank(),
        panel.grid.minor.y=element_blank())

plot_to_file(
  last_plot(), "./figures/candidate_topic_time_observation_count.pdf",
  .width_rel=1, .height_rel=0.35
)

################################################################################

roll_window_model_data %>%
  select(where(is.numeric)) %>%
  pivot_longer(everything(), names_to="name", values_to="value") %>%
  mutate(
    across(name, str_replace_all,
           c(".*divi$"="Polarisiertheit", ".*mean$"="Durchschnitt"))
  ) %>%
  ggplot(aes(x=value)) +
  geom_histogram(fill="grey70", size=0, bins=20) +
  geom_vline(
    aes(xintercept=value), linetype=2, color="grey20",
    data=~{summarise(group_by(.x, name), across(value, mean))}
  ) +
  # ggh4x::stat_theodensity(distri="t", start.arg=list(df=20, ncp=5)) +
  scale_y_continuous(position="right") +
  facet_wrap(facets=vars(fct_rev(name)), scales="free", ncol=1) +
  labs(x="Wert", y="Häufigkeit")

plot_to_file(
  last_plot(), "./figures/target_var_dist.pdf",
  .width_rel=0.3, .height_rel=0.5
)
