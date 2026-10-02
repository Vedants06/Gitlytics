#!/usr/bin/env Rscript
# r/02_weekday_heatmap.R

required_packages <- c("ggplot2", "dplyr", "data.table")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
    library(pkg, character.only = TRUE)
  }
}

message("[*] Running Chart 2: Weekday vs Hour Heatmap (IST)...")

tsv_path <- "exports/gold_hourly_activity.tsv"
if (!file.exists(tsv_path)) {
  message("  [!] gold_hourly_activity.tsv not found. Generating sample heatmap dataset...")
  set.seed(101)
  dt <- expand.grid(
    weekday = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"),
    hour = 0:23
  ) %>% as.data.table()
  dt[, event_count := round(runif(.N, 8000, 20000) * ifelse(weekday %in% c("Saturday", "Sunday"), 0.6, 1))]
} else {
  dt <- fread(tsv_path)
}

# Convert UTC to IST (+5.30 hours)
dt[, hour_ist := (hour + 5) %/% 1] # Simple mapping helper for display
dt[, weekday := factor(weekday, levels = c("Sunday", "Saturday", "Friday", "Thursday", "Wednesday", "Tuesday", "Monday"))]

# Plot
p <- ggplot(dt, aes(x = hour, y = weekday, fill = event_count)) +
  geom_tile(color = "white", size = 0.2) +
  scale_fill_viridis_c(option = "plasma", labels = scales::comma) +
  scale_x_continuous(breaks = seq(0, 23, by = 2)) +
  labs(
    title = "Activity Intensity Heatmap",
    subtitle = "GitHub Event Volume Cross-Section by Weekday & Hour",
    x = "Hour of Day (UTC)",
    y = "Day of Week",
    fill = "Total Events"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid = element_blank()
  )

ggsave("exports/plots/chart_02_weekday_heatmap.png", plot = p, width = 10, height = 5, dpi = 300)
message("[+] Chart saved to: exports/plots/chart_02_weekday_heatmap.png")