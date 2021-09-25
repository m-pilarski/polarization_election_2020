sq_trans <- scales::trans_new("sq", function(.v){.v^2}, function(.v){.v^(1/2)})

gg_theme <-
  theme_gray(base_family="Franziska Pro", base_size=8) +
  # theme(
  #   plot.background=element_blank(),
  #   plot.margin=unit(c(0, 4, 0, 4), "pt"),
  #   legend.background=element_blank(),
  #   legend.position="bottom",
  #   legend.key=element_blank(),
  #   legend.key.size=unit(8, "pt"),
  #   panel.background=element_blank(),
  #   panel.grid=element_line(color="gray95")
  # )
  theme(
    plot.background=element_blank(),
    plot.margin=margin(0, 0, 0, 0, "pt"),
    legend.background=element_blank(),
    legend.position="bottom",
    legend.key=element_blank(),
    legend.key.size=unit(8, "pt"),
    panel.grid=element_line(color="gray85"),
    panel.background=element_rect(fill="gray98", size=0),
    legend.title.align=0.5,
    strip.background=element_rect(fill="gray85", color="gray85")
  )
  

theme_set(gg_theme)

gg_theme_blank <-
  gg_theme +
  theme(axis.title=element_blank(),
        axis.text=element_blank(),
        axis.ticks=element_blank(),
        panel.grid=element_blank(),
        panel.background=element_blank())
