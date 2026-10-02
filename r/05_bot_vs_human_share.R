#!/usr/bin/env Rscript
# r/05_bot_vs_human_share.R

required_packages <- c("ggplot2", "dplyr", "data.table")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
    library(pkg, character.only = TRUE)
  }
}

message("[*] Running Chart 5: Bot vs Human Traffic Breakdown...")

# Using the pipeline 4 results (PushEvent and CreateEvent)
dt <- data.table(
  event_type = c("PushEvent", "PushEvent", "CreateEvent", "CreateEvent", "IssueCommentEvent", "IssueCommentEvent"),
  category = c("BOT", "HUMAN", "BOT", "HUMAN", "BOT", "HUMAN"),
  count = c(960385, 1747982, 56730, 408151, 40299, 74686)
)

# Compute percentage of total for each category within event type
dt[, group_sum := sum(count), by = event_type]
dt[, pct := (count / group_sum) * 100]

p <- ggplot(dt, aes(x = event_type, y = pct, fill = category)) +
  geom_bar(stat = "identity", position = "dodge", width = 0.6) +
  geom_text(aes(label = sprintf("%.1f%%", pct)), position = position_dodge(width = 0.6), vjust = -0.5, size = 3.5) +
  scale_fill_manual(values = c("#D55E00", "#009E73")) +
  labs(
    title = "Bot vs Human Traffic Share by Action",
    subtitle = "Validation of Ground-Truth [bot] Tagging",
    x = "Event Type",
    y = "Percentage of Events (%)",
    fill = "Actor Class"
  ) +
  scale_y_continuous(limits = c(0, 100)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

ggsave("exports/plots/chart_05_bot_vs_human_share.png", plot = p, width = 8, height = 5, dpi = 300)
message("[+] Chart saved to: exports/plots/chart_05_bot_vs_human_share.png")