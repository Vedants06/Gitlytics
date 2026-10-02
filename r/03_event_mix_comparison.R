#!/usr/bin/env Rscript
# r/03_event_mix_comparison.R

required_packages <- c("ggplot2", "dplyr", "data.table")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
    library(pkg, character.only = TRUE)
  }
}

message("[*] Running Chart 3: Event Type Shares...")

# We will read event percentages directly from MongoDB pipeline 1 results
# which we ran successfully: PushEvent 68.03%, CreateEvent 11.68%, etc.
dt <- data.table(
  type = c("PushEvent", "CreateEvent", "PullRequestEvent", "WatchEvent", "IssueCommentEvent", "Other"),
  percentage = c(68.03, 11.68, 5.10, 4.62, 2.89, 7.68)
)

dt[, type := reorder(type, percentage)]

p <- ggplot(dt, aes(x = type, y = percentage, fill = type)) +
  geom_bar(stat = "identity", alpha = 0.85, show.legend = FALSE) +
  geom_text(aes(label = sprintf("%.2f%%", percentage)), hjust = -0.1, size = 3.5) +
  coord_flip() +
  scale_fill_brewer(palette = "Blues") +
  labs(
    title = "Distribution of Public GitHub Event Types",
    subtitle = "Percentage Share based on ~4 Million Records",
    x = "Event Type",
    y = "Percentage (%)"
  ) +
  scale_y_continuous(limits = c(0, 80)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

ggsave("exports/plots/chart_03_event_mix_comparison.png", plot = p, width = 8, height = 5, dpi = 300)
message("[+] Chart saved to: exports/plots/chart_03_event_mix_comparison.png")