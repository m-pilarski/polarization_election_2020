if(grepl("(?i)windows", sessionInfo()$running)){
  setwd("I:/masterarbeit/analysis")
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
library(disk.frame)
library(kableExtra)

source("./resources/plot_to_file.R")
source("./resources/ggplot_theme_set.R")

################################################################################

data_dir <- path_wd("data")
temp_dir <- dir_create(path(data_dir, "tmp"), recurse=FALSE)

################################################################################

repl_graph_data <-
  disk.frame(path(data_dir, "02-convers_full_raw.df")) %>% 
  srckeep(c("convers_id", "status_id", "reply_to_status_id")) %>% 
  get_chunk(2) %>%
  group_by(convers_id) %>% 
  group_walk(function(.group_data, .group_keys){
    
    .repl_graph_edgelist <- 
      .group_data %>% 
      select(from = reply_to_status_id, to = status_id) %>% 
      mutate(across(everything(), as.character))
    
    .repl_graph_nodelist <- 
      .repl_graph_edgelist %>% 
      drop_na() %>% 
      distinct() %>% 
      as_tbl_graph() %>% 
      activate(nodes) %>%
      mutate(depth = node_distance_from(node_is_root()),
             size = local_size(order=max(depth), mode="out")) %>% 
      as_tibble() %>% 
      mutate(collapse = depth == 1 & size == 1)
    
    .repl_graph_edgelist <- 
      .repl_graph_edgelist %>% 
      group_by(collapse = 
                 to %in% filter(.repl_graph_nodelist, collapse)$name) %>% 
      group_modify(function(.group_data, .group_keys){
        if(.group_keys$collapse){
          .group_data <- 
            summarise(.group_data, from = unique(from), to = "collapsed")
        }
        return(.group_data)
      }) %>% 
      ungroup() %>% 
      select(-collapse) %>% 
      drop_na()
    
    .repl_graph_nodelist <- 
      .repl_graph_nodelist %>% 
      group_by(collapse) %>% 
      group_modify(function(.group_data, .group_keys){
        if(.group_keys$collapse){
          .group_data <- 
            summarise(.group_data, 
                      name = "collapsed", 
                      depth = unique(depth),
                      size = sum(size, na.rm=TRUE))
        }
        return(.group_data)
      }) %>% 
      ungroup() %>% 
      select(-collapse) %>% 
      drop_na()
    
    .repl_graph <- 
      .repl_graph_edgelist %>% 
      as_tbl_graph() %>% 
      left_join(.repl_graph_nodelist, by="name") %>% 
      activate(nodes) %>% 
      filter(name != "collapsed") %>% 
      create_layout("circlepack", weight=size) %>%
      ggraph() +
      geom_node_circle(aes(color=depth, fill=depth), size=0.25) +
      scale_fill_viridis_c(option="magma", trans="sqrt", begin=0.2, end=0.9,
                           na.value="transparent") +
      scale_color_viridis_c(option="magma", trans="sqrt", begin=0.3, end=1,
                            na.value="transparent") +
      scale_size_continuous(trans="sqrt") +
      guides(color = "none",
             fill = guide_colorbar(title.position="top", title.hjust=0.5,
                                   title.vjust=0.5, barheight=unit(5, "pt"),
                                   barwidth=unit(100, "pt"))) +
      labs(color = "Tiefe (Entfernung vom Wurzelknoten)") +
      theme(panel.spacing=unit(0, "pt")) +
      scale_x_continuous(expand=c(0, 0)) +
      scale_y_continuous(expand=c(0, 0))
    
    .plot_path <- 
      path("./figures/repl_graph/", .group_keys$convers_id, ext="pdf")
    
    dir_create(path_dir(.plot_path))
    
    plot_to_file(.repl_graph, .plot_path, .width_rel=1, .height_rel=1)
    
  }); beepr::beep()

###############################################################################

profile_images <- 
  c("JoeBiden", "KamalaHarris", "realDonaldTrump", "Mike_Pence") %>% 
  as_tibble_col("screen_name") %>% 
  transpose() %>%
  map_dfr(function(.transp_r){
    
    .image_rect_path <-
      glue("./data/00-profile_images/{.transp_r$screen_name}.jpg") %>%
      fs::path_abs() %>%
      as.character()

    .transp_r$profile_image_url %>%
      str_replace("_normal\\.jpg$", "_400x400.jpg") %>%
      httr::GET() %>%
      httr::content("raw") %>%
      write_file(.image_rect_path)
    
    .image_circ_path <- 
      glue("./data/00-profile_images/{.transp_r$screen_name}_circ.png") %>% 
      fs::path_abs() %>% 
      as.character()
    
    system(glue("convert {.image_rect_path} -alpha on \\( +clone -threshold ",
                "-1 -negate -fill white -draw \"circle 200,200 200,0\" \\) ",
                "-compose copy_opacity -composite {.image_circ_path}"))
    
    return(tibble(screen_name=.transp_r$screen_name, image=.image_circ_path))
    
  }) %>% 
  pull(image, name=screen_name)

tweets_from_latest <- 
  fs::dir_ls("./data/01-tweets_raw/", regexp="\\d{4}.rds$") %>% 
  nth(-1) %>% 
  read_rds() %>% 
  chuck("tweets") %>% 
  map("from_data") %>% 
  map_dfr(function(.from_data){
    .from_data %>% 
      collect() %>% 
      as_tibble()
  }) %>% 
  filter(!is_retweet) %>% 
  filter(created_at <= max(created_at[screen_name=="realDonaldTrump"])) %>% 
  slice_max(order_by=created_at, n=2e2) %>% 
  mutate(created_at = format(created_at),
         text = textutils::HTMLdecode(text))

tweets_from_latest %>% 
  add_column(profile_image = "") %>% 
  select(created_at, profile_image, text) %>% 
  kable(format="html", col.names=NULL, align="rcl") %>%
  column_spec(2, image=spec_image(recode(tweets_from_latest$screen_name, 
                                         !!!profile_images), 
                                  width=200, height=200)) %>% 
  kable_styling(bootstrap_options=c("striped", "hover")) %>% 
  save_kable("./tables/latest.html")