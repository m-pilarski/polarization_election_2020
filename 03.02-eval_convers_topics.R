if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("D:/Documents/uni/masterarbeit/analysis")
}else{
  setwd("~/Documents/uni/masterarbeit/analysis")
}

################################################################################

library(fs)
library(glue)
library(tidyverse)
library(magrittr)
library(bit64)
library(stm)
library(tidygraph)
library(ggraph)
library(ggiraph)
library(kableExtra)

# source("./resources/tidystm.R")
source("./resources/summarize_stm.R")
source("./resources/disk.frame_extensions.R")
source("./resources/plot_to_file.R")
source("./resources/ggplot_theme_set.R")

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

data_dir <- path_wd("data")
temp_dir <- set_temp_dir(dir_create(path(data_dir, "tmp")))

################################################################################
################################################################################
################################################################################

convers_info <- read_rds("./data/02-convers_info.rds")

convers_stm_data <- read_rds("./data/02-convers_stm_data.rds")

convers_stm_form <- list(prevalence=~screen_name*s(created_at_num),
                         content=~screen_name)

################################################################################

convers_stm_best <- read_rds("./data/03-convers_stm_best.rds")

################################################################################

convers_stm_best_summary <- 
  summarize_stm(.stm_fit=convers_stm_best, .stm_data=convers_stm_data)

################################################################################

convers_topic_shares <-
  bind_cols(
    stm::make.dt(convers_stm_best),
    transmute(read_rds(path(data_dir, "02-convers_stm_data.rds"))$meta,
              across(convers_id, bit64::as.integer64))
  ) %>% 
  select(convers_id, matches("^Topic\\d+$")) %>% 
  pivot_longer(-convers_id, names_to="topic_id", values_to="topic_prop") %>% 
  mutate(across(topic_id, function(.id){
    as.integer(str_extract(.id, "(?<=^Topic)\\d+$"))
  })) %>% 
  left_join(select(convers_info, convers_id, convers_size_raw),
            by="convers_id") %>% 
  mutate(topic_repl_count = topic_prop * convers_size_raw)

################################################################################

convers_stm_best_labels <-
  convers_stm_best_summary %>% 
  `[`(c("topic_terms_main", "topic_terms_inter")) %>% 
  bind_rows() %>% 
  mutate(across(screen_name, replace_na, "all")) %>% 
  filter(prob_rank <= 10) %>% 
  group_by(topic_id, screen_name) %>% 
  arrange(prob_rank) %>% 
  summarise(topic_labs = str_c(term, collapse=", "), .groups="drop") %>% 
  arrange(topic_id, screen_name)

convers_stm_best_labels %>% 
  filter(screen_name == "all") %>% 
  select(-screen_name) %>% 
  semi_join(filter(topic_group, topic_group != "anderes Thema"), 
            by="topic_id")

################################################################################

topic_group <-
  list(
    "Wahlbetrug" = list(32, 40, 45, 49, 53, 63), # 17, 27
    "struktureller Rassismus" = list(22, 47, 76), # 5
    "COVID-19-Pandemie" = list(26, 33, 35, 71, 72), # 54, 50
    "Wirtschaft" = list(8, 39) # 68
  ) %>% 
  enframe(name="topic_group", value="topic_id") %>% 
  unnest_longer(topic_id) %>% 
  mutate(across(topic_group, function(.topic_group){
    factor(.topic_group, levels=c(sort(unique(.topic_group)), "anderes Thema"))
  })) %>% 
  complete(topic_id = seq_len(ncol(convers_stm_best$theta)),
           fill=list("topic_group"="anderes Thema"))

################################################################################

stm_topic_stats <-
  convers_stm_best_summary$topic_shares %>% 
  full_join(convers_stm_best_summary$topic_sem_cohs, by="topic_id") %>%
  nest_join(convers_stm_best_summary$topic_terms_main,
            name="topic_terms_main", by="topic_id") %>%
  full_join(topic_group, by="topic_id") %>%
  write_rds("./data/03-stm_topic_stats.rds")

################################################################################
################################################################################
################################################################################

convers_stm_fit_corr <- topicCorr(convers_stm_best, method="huge")

set.seed(3)
topic_corr_network_plot <-
  convers_stm_fit_corr %>% 
  chuck("poscor") %>% 
  as.matrix() %>% 
  set_colnames(1:ncol(.)) %>% 
  set_rownames(1:nrow(.)) %>% 
  as_tbl_graph(directed=FALSE) %>%
  activate(nodes) %>%
  mutate(topic_id = as.integer(name)) %>% 
  select(-name) %>% 
  activate(edges) %>%
  filter(!edge_is_loop()) %>% 
  activate(nodes) %>% 
  left_join(topic_group, by="topic_id") %>% 
  left_join(
    summarize(group_by(convers_topic_shares, topic_id),
              across(c(topic_prop, topic_repl_count), sum)),
    by="topic_id"
  ) %>% 
  left_join(convers_stm_best_summary$topic_sem_cohs, by="topic_id") %>% 
  left_join(nest(convers_stm_best_summary$topic_terms_main, 
                 prob_terms=-topic_id), 
            by="topic_id") %>% 
  mutate(topic_labs = map_chr(prob_terms, function(.prob_terms){
    str_c(pull(slice_min(.prob_terms, prob_rank, n=20), term), collapse=", ")
  })) %>% 
  mutate(stress_x = create_layout(.G(), "stress", weights=weight)$x,
         stress_y = create_layout(.G(), "stress", weights=weight)$y) %>% 
  activate(edges) %>%
  mutate(in_group = map2_lgl(.N()$topic_group[from], .N()$topic_group[to], ~{
     (.x == .y) & (!"anderes Thema" %in% c(.x, .y))
  })) %>% 
  activate(nodes) %>% 
  arrange(topic_group != "anderes Thema", -c(topic_prop, topic_repl_count)[1]) %>% 
  activate(edges) %>% 
  filter(weight > 0.01) %>% 
  create_layout("manual", x=stress_x, y=stress_y) %>%
  ggraph() +
  geom_edge_link(aes(edge_width=weight, edge_alpha=in_group)) +
  geom_node_point(
    aes(size=topic_repl_count, fill=topic_group),
    shape=21, 
    color="white"
  ) +
  geom_point_interactive(
    aes(x=x, y=y, fill=topic_group, size=topic_repl_count,
        tooltip=str_c(topic_id, ": ", topic_labs), data_id=topic_id),
    shape=21,
    stroke=2
  ) +
  geom_node_text(
    aes(
      label=if_else(topic_group!="anderes Thema", as.character(topic_id), "")
    ), 
    color="white", family="Franziska Pro", 
    # size=2,
    size=3,
    fontface="bold.italic"
  ) +
  scale_fill_manual(
    values=
      topic_group$topic_group %>%
      unique() %>% 
      sort() %>% 
      discard(`==`, "anderes Thema") %>%
      (function(.vals){
        .vals %>%
          n_distinct() %>%
          # viridis::magma(begin=0.3, end=0.7) %>%
          viridis::magma(begin=0.2, end=0.8) %>%
          set_names(.vals)
      }) %>%
      append(c("anderes Thema"="grey80"))
  ) +
  scale_size_continuous(
    range=c(0,10), 
    limits=function(.r){assign_in(.r, 1, 0)},
    breaks=scales::pretty_breaks(3),
    labels=function(.b){prettyNum(.b, scientific=FALSE, big.mark="\\\\,")}
  ) +
  scale_edge_width_continuous(
    range=c(0.1,10), 
    limits=function(.r){assign_in(.r, 1, 0)},
  ) +
  scale_edge_alpha_manual(values=c("TRUE"=0.8, "FALSE"=0.2), guide="none") +
  guides(
    edge_width=guide_legend(direction="horizontal", title.position="top", 
                            title.hjust=0.5, order=3),
    fill=guide_legend(ncol=2, title.position="top", title.hjust=0.5,
                      byrow=TRUE, order=1),
    size=guide_legend(title.position="top", title.hjust=0.5, order=2)
  ) +
  coord_equal() +
  labs(
    fill="Themenkomplex", 
    size="Geschätzte Anzahl von Tweets", 
    edge_width="Korrelation") +
  theme(legend.box="vertical", 
        legend.direction="horizontal",
        legend.spacing=unit(0, "mm"),
        panel.spacing=unit(0, "pt"), 
        plot.margin=margin(0, 0, 0, -2.5, "pt"))

topic_corr_network_plot_legend_1 <- cowplot::get_legend(
  topic_corr_network_plot + 
    guides(edge_width="none", size="none") +
    theme(
      legend.text=element_text(margin=margin(0,10,0,0))
    )
)
topic_corr_network_plot_legend_2 <- cowplot::get_legend(
  topic_corr_network_plot + guides(fill="none") + theme()
)

topic_corr_network_plot_aligned <- 
  patchwork::wrap_plots(
    topic_corr_network_plot + theme(legend.position="none"),
    patchwork::wrap_plots(
      patchwork::plot_spacer(),
      patchwork::wrap_plots(
        topic_corr_network_plot_legend_1,
        patchwork::plot_spacer(),
        ncol=1, heights=c(4, 0.8)
      ), 
      topic_corr_network_plot_legend_2,
      patchwork::plot_spacer(),
      nrow=1, widths=c(1, 3, 3, 1)
    ),
    ncol=1, heights=c(1, 0.2)
  ); topic_corr_network_plot_aligned


plot_to_file(topic_corr_network_plot_aligned,
             path_wd("figures/topic_corr_network_plot_2.pdf"),
             .width_rel=1, .height_rel=1.12)

topic_corr_network_plot_i <- 
  girafe(ggobj=topic_corr_network_plot, width_svg=5, height_svg=5,
         options=list(opts_sizing(rescale=TRUE),
                      opts_hover(css="fill:white;stroke:black;"))); topic_corr_network_plot_i

htmlwidgets::saveWidget(topic_corr_network_plot_i,
                        "./topic_corr_network_plot_i.html")

################################################################################
################################################################################
################################################################################

screen_name_colors <- 
  c("#000000FF", viridisLite::magma(4, begin=0.2, end=0.7)) %>% 
  str_sub(start=2, end=7) %>% 
  set_names(unique(convers_stm_best_labels$screen_name))

convers_stm_best_labels_tab_sub <-
  convers_stm_best_summary %>% 
  pluck("topic_terms_main") %>% 
  filter(prob_rank <= 7) %>% 
  group_by(topic_id) %>% 
  summarise(topic_labs = linebreak(str_c(term, collapse=", ")),
            .groups="drop") %>% 
  left_join(topic_group, by="topic_id") %>% 
  filter(topic_group != "anderes Thema") %>% 
  (function(.data){
    
    .data %>% 
      arrange(topic_group) %>% 
      select(topic_id, topic_labs) %>%
      kable(
        col.names=c("", ""), format="latex", 
        caption=str_c("Begriffe mit den höchsten $\\theta$-Ge\\-wich\\-ten ",
                      "für die einzelnen Themen aus den fokussierten ",
                      "Themenkomplexen."),
        escape=TRUE, booktabs=TRUE, linesep="\\addlinespace[0.5em]"
      ) %>% 
      kable_styling(
        latex_options=c("striped"), font_size=8, full_width=TRUE
      ) %>% 
      pack_rows(index=table(fct_drop(.data$topic_group)), indent=FALSE) %>% 
      column_spec(1, "5mm") %>% 
      str_remove_all("\\\\\\w+rule\\b") %>%
      str_remove_all("\\s*\\\\(begin|end)\\{table\\}\\S*\\s*") %>%
      str_replace_all("\\\\caption\\{", "\\\\captionof{table}{") %>%
      write_file("./tables/convers_stm_best_labels_tab_sub.tex")
    
  })

convers_stm_best_labels_tab_full <-
  convers_stm_best_summary %>% 
  pluck("topic_terms_main") %>% 
  filter(prob_rank <= 20) %>% 
  group_by(topic_id) %>% 
  summarise(topic_labs = linebreak(str_c(term, collapse=", ")),
            .groups="drop") %>% 
  left_join(topic_group, by="topic_id") %>% 
  (function(.data){
    
    .data %>% 
      arrange(topic_group) %>% 
      select(topic_id, topic_labs) %>%
      kable(
        col.names=c("", ""), format="latex", longtable=TRUE,
        caption=str_c("Begriffe mit den höchsten $\\theta$-Ge\\-wich\\-ten ",
                      "für die einzelnen Themen aus den fokussierten ",
                      "Themenkomplexen."),
        escape=TRUE, booktabs=TRUE, linesep="\\addlinespace[0.5em]"
      ) %>% 
      kable_styling(
        latex_options=c("striped"), font_size=8
      ) %>% 
      pack_rows(index=table(fct_drop(.data$topic_group)), indent=FALSE) %>% 
      column_spec(1, latex_column_spec="p{5mm}") %>%
      column_spec(2, latex_column_spec="p{148mm}") %>% 
      str_remove_all("(?<=\\n)\\\\\\w*rule.*?\\n") %>% 
      write_file("./tables/convers_stm_best_labels_tab_full.tex")
    
  })


################################################################################
#### DETERMINE TOPIC PROPORTIONS FOR THE SAMPLE TWEETS #########################
################################################################################

# reset_workers_df(.change_workers=5)
# setup_disk.frame(workers=5)

# convers_full_topic_props <-
#   disk.frame(path(data_dir, "02-convers_full_term_count.df")) %>%
#   reset_workers_df() %>%
#   resuming_cmap(function(.chunk){
# 
#     library(stm)
# 
#     .chunk_stm_data <-
#       .chunk %>%
#       dplyr::bind_rows(tibble::tibble(term = convers_stm_data$vocab)) %>%
#       tidytext::cast_dfm(document=status_id, term=term, value=count) %>%
#       quanteda::dfm_keep(convers_stm_data$vocab, valuetype="fixed") %>%
#       quanteda::dfm_subset(quanteda::rowSums(.) > 0) %>%
#       (function(.dfm){
#         .dfm <- .dfm[, order(quanteda::featnames(.dfm))]
#         .dfm <- as(.dfm, "dgTMatrix")
#         .docs <- stm:::ijv.to.doc(.dfm@i+1, .dfm@j+1, .dfm@x)
#         names(.docs) <- rownames(.dfm)
#         .stm_data <- list(documents=.docs, vocab=colnames(.dfm))
#         return(.stm_data)
#       }) %>%
#       stm::alignCorpus(convers_stm_best$vocab)
# 
#     .convers_stm_data_meta <-
#       dplyr::select(convers_stm_data$meta, -tidyselect::any_of("text")) %>%
#       dplyr::mutate(
#         convers_id = bit64::as.integer64(names(convers_stm_data$documents))
#       )
# 
#     .chunk_stm_data$meta <-
#       tibble::tibble(status_id =
#                        as.integer64(names(.chunk_stm_data$documents))) %>%
#       dplyr::left_join(dplyr::distinct(.chunk, status_id, convers_id),
#                        by="status_id") %>%
#       left_join(.convers_stm_data_meta, by="convers_id") %>%
#       as.data.frame()
# 
#     .chunk_topic_prop <-
#       stm::fitNewDocuments(model=convers_stm_best,
#                            documents=.chunk_stm_data$documents,
#                            newData=.chunk_stm_data$meta,
#                            origData=convers_stm_data$meta,
#                            prevalence=
#                              convers_stm_best$settings$covariates$formula,
#                            betaIndex=.chunk_stm_data$meta$screen_name,
#                            prevalencePrior="None",#"Covariate",
#                            contentPrior="Covariate",
#                            test=TRUE,
#                            verbose=FALSE) %>%
#       purrr::chuck("theta") %>%
#       magrittr::set_rownames(names(.chunk_stm_data$documents)) %>%
#       magrittr::set_colnames(1:ncol(.)) %>%
#       tibble::as_tibble(rownames="status_id") %>%
#       dplyr::mutate(dplyr::across(status_id, bit64::as.integer64)) %>%
#       tidyr::pivot_longer(-status_id, names_to="topic_id",
#                           values_to="topic_prop") %>%
#       dplyr::mutate(dplyr::across(topic_id, as.integer))
# 
#     .chunk_mod <-
#       .chunk %>%
#       dplyr::distinct(convers_id, status_id) %>%
#       dplyr::left_join(.chunk_topic_prop, by="status_id") %>%
#       data.table::as.data.table()
# 
#     return(.chunk_mod)
# 
#   }, .resume=TRUE, .workers=5, .scheduling=Inf) %>%
#   write_disk.frame(outdir=path(data_dir, "03-convers_full_topic_props.df"),
#                    overwrite=TRUE)

# reset_workers_df(.change_workers=1)

################################################################################

# reset_workers_df(.change_workers=5)

setup_disk.frame(workers=1)

convers_full_topic_group <-
  disk.frame(path(data_dir, "03-convers_full_topic_props.df")) %>% 
  reset_workers_df() %>% 
  resuming_cmap(function(.chunk){
    
    .chunk_mod <- 
      .chunk %>% 
      left_join(topic_group, by="topic_id") %>% 
      group_by(status_id, status_topic_group = topic_group) %>% 
      summarise(status_topic_group_prop = sum(topic_prop), .groups="drop") %>% 
      group_by(status_id) %>% 
      slice_max(order_by=status_topic_group_prop, n=1, with_ties=FALSE) %>% 
      ungroup()
    
    gc()
    
    return(.chunk_mod)
    
  }, .resume=TRUE, .workers=5, .scheduling=Inf) %>% 
  reset_workers_df() %>%
  write_disk.frame(outdir=path(data_dir, "03-convers_full_topic_group.df"),
                   overwrite=TRUE)

reset_workers_df(.change_workers=1)

################################################################################
################################################################################
################################################################################

disk.frame(path(data_dir, "03-convers_full_topic_group.df")) %>% 
  cmap_dfr(~select(.x, status_topic_group, status_topic_group_prop)) %>% 
  filter(status_topic_group != "anderes Thema") %>% 
  (function(.data){
    .data %>% 
      group_by(status_topic_group_prop >= 0.5) %>% 
      summarise(count = n(), .groups="drop") %>% 
      mutate(share = count/sum(count)) %>% 
      print()
    return(.data)
  }) %>% 
  ggplot(aes(x=status_topic_group_prop, group=status_topic_group,
             fill=status_topic_group)) +
  geom_density(alpha=0.5, size=0) +
  geom_vline(xintercept=0.5) +
  facet_wrap(vars(status_topic_group))
