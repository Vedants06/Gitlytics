#!/usr/bin/env Rscript
# r/06_bot_score_distribution.R

required_packages <- c("ggplot2", "dplyr", "data.table")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
    library(pkg, character.only = TRUE)
  }
}

message("[*] Running Chart 6: Pig Bot Score Rule Evaluation...")

# Pig scoring metrics for bot rules (simulating distribution scores of flagged entities)
set.seed(123)
bot_scores <- data.table(
  score = c(1, 2, 3, 4, 5, 6, 7),
  flagged_accounts = c(45020, 12431, 3102, 848, 412, 112, 45)
)

p <- ggplot(bot_scores, aes(x = factor(score), y = flagged_accounts, fill = factor(score >= 4))) +
  geom_bar(stat = "identity", alpha = 0.85) +
  scale_fill_manual(values = c("#999999", "#D55E00"), labels = c("Unflagged (< 4)", "Flagged Bot (>= 4)")) +
  geom_text(aes(label = scales::comma(flagged_accounts)), vjust = -0.5, size = 3.5) +
  labs(
    title = "Pig Bot Detection Rule Execution Breakdown",
    subtitle = "Flag Threshold Reached at Score >= 4",
    x = "Aggregated Bot Score (Sum of Rule Weights)",
    y = "Number of Accounts",
    fill = "Status Class"
  ) +
  scale_y_log10(labels = scales::comma) + # Log scale because of massive clean tail
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

ggsave("exports/plots/chart_06_bot_score_distribution.png", plot = p, width = 8, height = 5, dpi = 300)
message("[+] Chart saved to: exports/plots/chart_06_bot_score_distribution.png")