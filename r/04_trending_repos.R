#!/usr/bin/env Rscript
# r/04_trending_repos.R

required_packages <- c("ggplot2", "dplyr", "data.table")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
    library(pkg, character.only = TRUE)
  }
}

message("[*] Running Chart 4: Top Starred Repositories...")

# Plotting the top starred repos from MongoDB Pipeline 2
dt <- data.table(
  repo = c("tw93/Pake", "2Retr0/GodotOceanWaves", "WinampDesktop/winamp", "unclecode/crawl4ai", "better-auth/better-auth", "meta-llama/llama-stack", "nadimkobeissi/mkbsd", "immich-app/immich", "localsend/localsend", "mediar-ai/screenpipe"),
  stars = c(993, 739, 560, 524, 506, 401, 365, 283, 232, 209)
)

dt[, repo := reorder(repo, stars)]

p <- ggplot(dt, aes(x = repo, y = stars, fill = stars)) +
  geom_bar(stat = "identity", width = 0.7, show.legend = FALSE) +
  geom_text(aes(label = scales::comma(stars)), hjust = -0.1, size = 3.5) +
  coord_flip() +
  scale_fill_gradient(low = "#56B4E9", high = "#0072B2") +
  labs(
    title = "Top Starred Repositories",
    subtitle = "Based on Unique Star Event Registrations",
    x = "Repository",
    y = "Stars Received"
  ) +
  scale_y_continuous(limits = c(0, 1100)) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

ggsave("exports/plots/chart_04_top_starred_repos.png", plot = p, width = 9, height = 5, dpi = 300)
message("[+] Chart saved to: exports/plots/chart_04_top_starred_repos.png")