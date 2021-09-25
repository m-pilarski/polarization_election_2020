if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("D:/Documents/uni/masterarbeit/analysis/")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

library(glue)
library(fs)
library(tidyverse)
library(magrittr)
library(lubridate)
library(disk.frame)

source("./resources/best_map.R")
source("./resources/disk.frame_extensions.R")
source("./resources/ggplot_theme_set.R")
source("./resources/plot_to_file.R")

################################################################################

data_dir <- path_wd("data")
temp_dir <- dir_create(path(data_dir, "tmp"), recurse=FALSE)

################################################################################

election_day <- ymd("2020-11-03", tz="EST")
study_tscope <- interval(election_day-weeks(10), ymd("2021-01-06", tz="EST"))

################################################################################

convers_info <- read_rds(path(data_dir, "02-convers_info.rds"))

################################################################################

convers_info %>% 
  filter(convers_start_at %within% study_tscope) %>% 
  group_by(convers_candidate, 
           convers_start_at_date = date(convers_start_at)) %>% 
  summarise(convers_count = n_distinct(convers_id), .groups="drop") %>% 
  ggplot(aes(x=convers_count)) +
  geom_histogram(color="white", fill="gray70") + 
  geom_vline(
    aes(xintercept=convers_count),
    data=~{
      summarise(group_by(.x, convers_candidate), 
                convers_count=mean(convers_count))
    },
    color="#00AD80", linetype=2
  ) +
  labs(x="Anzahl der begonnenen Konversationen pro Tag", 
       y="Häufigkeit") +
  facet_wrap(vars(str_escape_tex(convers_candidate)), nrow=1)

plot_to_file(last_plot(), "./figures/convers_count.pdf",
             .width_rel=1, .height_rel=0.2)

convers_info %>% 
  filter(convers_start_at %within% study_tscope) %>% 
  group_by(convers_candidate, 
           convers_start_at_date = date(convers_start_at)) %>% 
  summarise(convers_count = n_distinct(convers_id), .groups="drop") %>% 
  group_by(convers_candidate) %>% 
  summarise(convers_count_daily_mean = mean(convers_count), .groups="drop")

################################################################################

disk.frame(path(data_dir, "02-convers_full_raw.df")) %>% colnames()
  cmap_dfr(~{distinct(.x, user_id)}) %>% 
  distinct(user_id)

################################################################################

disk.frame(path(data_dir, "02-convers_full_raw.df")) %>% 
  cmap_dfr(function(.chunk){
    .chunk %>% 
      group_by(convers_id) %>% 
      transmute(created_after_hours = 
                  time_length(created_at - created_at[status_id == convers_id],
                              "hours"))
  }) %>% 
  summarise(after_1hour = 
              sum(created_after_hours <= 1)/length(created_after_hours),
            after_12hour = 
              sum(created_after_hours/12 <= 1)/length(created_after_hours),
            after_1day = 
              sum(created_after_hours/24 <= 1)/length(created_after_hours),
            after_2day = 
              sum(created_after_hours/(24*2) <= 1)/length(created_after_hours),
            after_1week = 
              sum(created_after_hours/(24*7) <= 1)/length(created_after_hours))

################################################################################

reset_workers_df(.change_workers=5)

candidate_status_colloc_share <- 
  disk.frame(path(data_dir, "02-convers_full_term_count.df")) %>%
  reset_workers_df() %>%
  cmap_dfr(function(.chunk){
    summarise(group_by(filter(.chunk, status_id == convers_id), status_id),
              colloc_share = sum(str_detect(term, " "))/length(term))
  }) %>%
  reset_workers_df() %>%
  arrange(-colloc_share)

candidate_status_max_colloc_clean <- 
  disk.frame(path(data_dir, "02-convers_full_clean.df")) %>% 
  reset_workers_df() %>% 
  cmap_dfr(function(.chunk){
    filter(.chunk, status_id %in% candidate_status_colloc_share$status_id)
  }) %>% 
  reset_workers_df()

candidate_status_max_colloc_token <- 
  disk.frame(path(data_dir, "02-convers_full_token.df")) %>% 
  reset_workers_df() %>% 
  cmap_dfr(function(.chunk){
    filter(.chunk, status_id %in% candidate_status_colloc_share$status_id)
  }) %>% 
  reset_workers_df()

candidate_status_max_colloc_term_count <- 
  disk.frame(path(data_dir, "02-convers_full_term_count.df")) %>% 
  reset_workers_df() %>% 
  cmap_dfr(function(.chunk){
    filter(.chunk, status_id %in% candidate_status_colloc_share$status_id)
  }) %>% 
  reset_workers_df()

reset_workers_df(.change_workers=1)

candidate_status_max_colloc <- 
  candidate_status_colloc_share %>% 
  inner_join(transmute(candidate_status_max_colloc_clean, status_id, created_at,
                       text_clean = text), by="status_id") %>% 
  inner_join(transmute(candidate_status_max_colloc_token, status_id, 
                       text_token = text), by="status_id") %>% 
  inner_join(select(nest(candidate_status_max_colloc_term_count, 
                         text_term_count = c(term, count)), 
                    status_id, text_term_count),
                    by="status_id") %>% 
  as_tibble()

candidate_status_max_colloc %>% 
  # filter(emo::ji_detect(text_clean)) %>% 
  filter(str_detect(text_token, "\\bfraud\\b")) %>% 
  mutate(text_term_count = map_chr(text_term_count, ~{
    # str_c(str_c(",,", .x$term, "''"), collapse=", ")
    # str_c(str_c(.x$term, ": ", .x$count), collapse=", ")
    str_c("\\{", str_c(.x$term, collapse=", "), "\\}")
  })) %>% 
  transpose() %>% 
  map_chr(~{
    .x[str_which(names(.x), "^text_")] %>% 
      flatten_chr() %>% 
      str_c("\\item ", .) %>% 
      c("") %>% 
      str_c(collapse="\n")
  }) %>% 
  write_lines("test")

###############################################################################
################################################################################
################################################################################

library(tidygraph)
library(ggraph)

from_data <- 
  tweets$tweets[[3]]$from_data %>% 
  select(status_id, reply_to_status_id) %>% 
  collect() %>% 
  select(from = reply_to_status_id, to = status_id) %>% 
  filter(is.na(from))

repl_data <- 
  tweets$tweets[[3]]$repl_data %>% 
  filter(thread_id == from_data$to[1000]) %>% 
  select(status_id, reply_to_status_id) %>% 
  collect() %>% 
  select(from = reply_to_status_id, to = status_id)

repl_data_plot <- 
  repl_data %>% 
  as_tbl_graph() %>% 
  activate(nodes) %>%
  mutate(depth = node_distance_from(node_is_root()),
         size = local_size(order=max(depth), mode="out")) %>%
  ggraph("circlepack", weight=size) +
  geom_node_circle(aes(fill=depth + 2, color=depth), size=0.2) +
  scale_fill_viridis_c(option="magma", trans="sqrt", begin=0.1,
                       na.value="transparent",
                       limits=function(.l){.l[1] <- .l[1] - 2; .l}) +
  scale_color_viridis_c(option="magma", trans="sqrt", begin=0.1,
                        na.value="transparent",
                        limits=function(.l){.l[2] <- .l[2] + 2; .l}) +
  scale_size_continuous(trans="sqrt") +
  guides(fill = "none") +
  coord_equal()

ggsave("test.png", repl_data_plot, width=15, height=15, units="cm")


repl_graphs <-
  .repl_data %>%
  select_at(vars(-matches("^reply_to_status_id_root$"))) %>%
  left_join(.tweets_repl_data_keep %>%
              unnest(reply_id) %>%
              select(reply_to_status_id_root = status_id,
                     status_id = reply_id),
            by="status_id") %>%
  select(from = reply_to_status_id, to = status_id,
         root = reply_to_status_id_root) %>%
  group_by(root) %>%
  filter(n() > 10) %>%
  # filter(!all(from %in% .from_data$status_id)) %>%
  # filter(from %in% unique(.$from)[1:10]) %>%
  group_map(function(.tweets, ...){

    # test <<- .tweets
    # .tweets <- test

    .repl_graph <-
      as_tbl_graph(.tweets) %>%
      activate(nodes) %>%
      mutate(#degree = centrality_degree(mode="out") + 1,
             depth = node_distance_from(node_is_root()) + 1,
             size = local_size(order=max(depth), mode="out"))# %>%

    .repl_graph_plot <-
      .repl_graph %>%
      activate(nodes) %>%
      # ggraph("stress") +
      ggraph("circlepack", weight=size) +
      # geom_edge_elbow(color="gray70") +
      # geom_node_point(aes(size = degree, fill = level),
      #                 shape=21, color="white", stroke=0.5) +
      geom_node_circle(aes(fill=depth), color="white", size=0.2) +
      scale_fill_viridis_c(option="magma", trans="sqrt", begin=0.2,
                           na.value="transparent") +
      scale_color_viridis_c(option="magma", trans="sqrt", begin=0.2,
                            na.value="transparent") +
      scale_size_continuous(trans="sqrt") +
      # guides(fill = NULL) +
      coord_equal()

    ggsave(str_c("./figures/repl_to_", ..1, ".pdf"),
           width=15, height=15, units="cm")

  })

################################################################################
################################################################################
################################################################################

disk.frame(path(data_dir, "02-convers_full_raw.df")) %>% 
  cmap_dfr(function(.chunk){
    .chunk %>% 
      left_join(
        select(convers_info, convers_id, convers_candidate), 
        by="convers_id"
      ) %>% 
      count(
        convers_candidate,
        status_is_convers_start = status_id == convers_id,
        status_created_at_date = 
          date(created_at),
        status_created_at_hour_intv = 
          factor(hour(created_at) %/% 6, levels=0:3,
                 labels=c("[00:00; 06:00)", "[06:00; 12:00)", 
                          "[12:00; 18:00)", "[18:00; 00:00)"))
        
      )
  }) %>% 
  group_by(across(-n)) %>% 
  summarise(across(n, sum), .groups="drop") %>% 
  filter(status_created_at_date %within% study_tscope) %>% 
  mutate(n = if_else(status_is_convers_start, n, -n)) %>% 
  ggplot(
    aes(x=status_created_at_date, y=n, grup=status_created_at_hour_intv, 
        fill=status_created_at_hour_intv)
  ) +
  geom_vline(xintercept=as_date(election_day), color="#00AD80", linetype=2) +
  geom_bar(stat="identity", position=position_stack(reverse=TRUE)) +
  ggh4x::facet_nested(
    rows=vars(str_escape_tex(convers_candidate), 
              factor(status_is_convers_start, levels=c(TRUE, FALSE),
                     labels=c("Konversationen", "Antworten"))), 
    scales="free_y"
  ) +
  scale_fill_viridis_d(option="magma", begin=0.2, end=0.8, direction=-1) +
  scale_y_continuous(labels=function(.b){
    format(abs(.b), big.mark="\\\\,", scientific=FALSE)
  }) + 
  scale_x_date() +
  labs(x="Datum", y="Anzahl", fill="Uhrzeit (EST)") +
  guides(fill=guide_legend(title.position="top", title.hjust=0.5)) + 
  theme(legend.key.size=unit(0.3, "cm"),
        legend.text=element_text(margin=margin(0, 5, 0, 0, "pt")),
        plot.margin=margin(2, 0, 0, 1, "pt"),
        panel.grid.minor.y=element_blank())

plot_to_file(last_plot(), "./figures/tweets_count_plot.pdf", .height_rel=1.25)

################################################################################

convers_info %>% 
  filter(convers_start_at %within% study_tscope) %>% 
  group_by(convers_candidate) %>% 
  summarise(across(convers_size_raw, 
                   list(mean=mean,
                        min=min,
                        max=max,
                        quantile=~list(quantile(.x, c(0.25, 0.5, 0.75)))),
                   .names="{.fn}")) %>% 
  unnest_wider(quantile, names_sep="_") %>% 
  select(
    Kandidat = convers_candidate,
    Durschnitt = mean,
    Minimum = min,
    starts_with("quantile"),
    Maximum = max
  ) %>% 
  mutate(across(where(is.numeric), round, digits=0)) %>% 
  kableExtra::kable(
    format="latex",
    escape=TRUE, booktabs=TRUE, linesep="\\addlinespace[0.5em]"
  ) %>% 
  kableExtra::kable_styling(
    latex_options=c("striped"), font_size=8
  ) %>% 
  str_remove_all("(?<=\\n)\\\\\\w*rule.*?\\n") %>% 
  write_file("./tables/convers_size_raw_summary.tex")

convers_info %>% 
  filter(convers_start_at %within% study_tscope) %>% 
  count(convers_candidate, convers_start_day=date(convers_start_at), 
        name="convers_count_day") %>% 
  complete(convers_candidate, convers_start_day, 
           fill=list(convers_count_day=0)) %>% 
  group_by(convers_candidate) %>% 
  summarise(across(convers_count_day, 
                   list(mean=mean,
                        min=min,
                        max=max,
                        quantile=~list(quantile(.x, c(0.25, 0.5, 0.75)))),
                   .names="{.fn}")) %>% 
  unnest_wider(quantile, names_sep="_") %>% 
  select(
    Kandidat = convers_candidate,
    Durschnitt = mean,
    Minimum = min,
    starts_with("quantile"),
    Maximum = max
  ) %>% 
  mutate(across(where(is.numeric), round, digits=1)) %>% 
  kableExtra::kable(
    format="latex",
    escape=TRUE, booktabs=TRUE, linesep="\\addlinespace[0.5em]"
  ) %>% 
  kableExtra::kable_styling(
    latex_options=c("striped"), font_size=8
  ) %>% 
  str_remove_all("(?<=\\n)\\\\\\w*rule.*?\\n") %>% 
  write_file("./tables/convers_count_day_summary.tex")
