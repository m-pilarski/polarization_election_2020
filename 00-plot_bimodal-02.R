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

sim_dist_data <- 
  list(
    "unimodal"=
      truncnorm::rtruncnorm(1e6, -1, 1, 0, 0.25),
    "unimodal_left"=
      truncnorm::rtruncnorm(1e6, -1, 1, -0.75, 0.25),
    "bimodal"=
      c(truncnorm::rtruncnorm(5e5, -1, 1, -0.5, 0.25),
        truncnorm::rtruncnorm(5e5, -1, 1, 0.5, 0.25)),
    "gleich"=
      runif(1e6, -1, 1),
    "bimodal_right"=
      c(truncnorm::rtruncnorm(3e5, -1, 1, -0.75, 0.25),
        truncnorm::rtruncnorm(7e5, -1, 1, 0.75, 0.25))
  ) %>% 
  enframe(name="distribution", value="sample") %>% 
  mutate(divi = map_dbl(sample, divisiveness_coefficient)) %>% 
  mutate()

sim_dist_plot <- 
  sim_dist_data %>% 
  unnest_longer(sample, values_to="value") %>% 
  ggplot(aes(x=value, y=..ndensity..)) +
  # stat_density(
  #   n=11, geom="bar",
  #   position=position_dodge(width=0.9),
  #   # breaks=seq(-1, 1, length.out=20),
  #   fill="gray70"
  # ) +
  geom_histogram(
    bins=20,
    # breaks=seq(-1, 1, length.out=20),
    color="gray98", fill="gray70"
  ) +
  scale_y_continuous(breaks=c(0, 1), labels=c("0", "max.")) +
  facet_wrap(
    vars(
      divi %>% 
        round(2) %>% 
        factor() %>% 
        fct_relabel(~str_c("$\\mathrm{PK} = ", .x, "$"))
    ), 
    nrow=1
  ) +
  labs(x="Wert", y="Häufigkeitsdichte") +
  theme(
    plot.margin=margin(0, 0, 0, 1, "pt"),
    panel.grid.minor.y=element_blank()
  ); sim_dist_plot

plot_to_file(sim_dist_plot, "./figures/sim_dist_plot.pdf", 
             .width_rel=1, .height_rel=0.2)

