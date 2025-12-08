library(tidyverse)
library(jsonlite)
library(lubridate)
library(lme4)
library(lmerTest)
library(mixedpower)
library(simr)

print("Starting Data Processing...")

# ==============================================================================
# PART 2: DATA LOADING & CLEANING
# ==============================================================================

# 1. Load CSVs
df <- read_csv("../SurveyResults.csv", show_col_types = FALSE)
df <- df[-c(1, 2), ] # Drop first two rows (Qualtrics metadata)
df$`Duration (in seconds)` <- as.integer(df$`Duration (in seconds)`)

# 2. Define Regex and Helper Functions (Robust Version)
simple_chatbots <- c("grok", "gemini", "perplexity", "claude", "chatgpt", "copilot")
simple_pattern <- paste(simple_chatbots, collapse = "|")

is_chatbot_url <- function(url) {
  if (is.na(url)) return(FALSE)
  if (grepl(simple_pattern, url, ignore.case = TRUE)) return(TRUE)
  if (grepl("^(?!.*udm=)(?=.*google).*$", url, perl = TRUE, ignore.case = TRUE)) return(TRUE)
  return(FALSE)
}

process_extension_logs <- function(response_id) {
  questions <- paste0("Q", 1:6)
  
  # Initialize with ID (as list so it survives list-wrapping later)
  res <- list(PID_Temp_Join_Key = response_id)
  
  for (q in questions) {
    res[[paste0(q, "_URLs_Clicked")]] <- list()
  }
  
  # Robust file finding
  log_path <- file.path("../ExtensionLogs", paste0(response_id, ".csv"))
  if (!file.exists(log_path)) {
    all_files <- list.files("ExtensionLogs", full.names = TRUE)
    target <- all_files[grepl(paste0("/", response_id, ".csv$"), all_files, ignore.case = TRUE)]
    if(length(target) > 0) log_path <- target[1]
  }
  
  if (file.exists(log_path)) {
    logs <- read_csv(log_path, show_col_types = FALSE)
    for(i in seq_len(nrow(logs))) {
      q <- trimws(as.character(logs$question_id[i]))
      if (q %in% questions) {
        res[[paste0(q, "_URLs_Clicked")]] <- append(res[[paste0(q, "_URLs_Clicked")]], logs$url[i])
      }
    }
  }
  
  # Wrap in tibble, ensuring lists are preserved as list-columns
  res_tibble <- as_tibble(lapply(res, list))
  res_tibble$PID_Temp_Join_Key <- unlist(res_tibble$PID_Temp_Join_Key)
  
  return(res_tibble)
}

print("... Processing Extension Logs")
extension_data <- map_dfr(df$ResponseID, process_extension_logs)

# Join logs to main df
df <- df %>% 
  mutate(PID_Temp_Join_Key = ResponseID) %>%
  left_join(extension_data, by = "PID_Temp_Join_Key") %>%
  select(-PID_Temp_Join_Key)

# 3. Numeric Conversions & Scales
acc_cols_new <- paste0("Q", 1:6, "_Acc")
df <- df %>% rename_with(~ acc_cols_new, all_of(paste0("Acc_Q", 1:6)))

# Define column groups
warmth_cols <- paste0("W_Q1_", 1:4)
dl_cols <- paste0("DL_Q1_", 1:7)
trust_cols <- paste0("T_Q1_", 1:4)
diff_cols <- paste0("Q", 1:6, "_Difficulty")
fam_cols <- paste0("Q", 1:6, "_Familiarity")
conf_cols <- paste0("Q", 1:6, "_Confidence_1")

numeric_targets <- c(warmth_cols, dl_cols, trust_cols, acc_cols_new, diff_cols, fam_cols, conf_cols)

# Force Numeric Conversion (Fixes "x must be numeric" error)
df <- df %>% mutate(across(any_of(numeric_targets), ~ suppressWarnings(as.numeric(.))))
df <- df %>% mutate(across(any_of(numeric_targets), ~ replace_na(., 0)))

# Calculate Means
df$UF_Q1 <- as.integer(df$UF_Q1)
df$Warmth <- rowMeans(df[warmth_cols], na.rm = TRUE)
df$DL <- rowMeans(df[dl_cols], na.rm = TRUE)
df$Trust <- rowMeans(df[trust_cols], na.rm = TRUE)
df$Accuracy <- rowMeans(df[acc_cols_new], na.rm = TRUE)
df$Difficulty <- rowMeans(df[diff_cols], na.rm = TRUE)
df$Familiarity <- rowMeans(df[fam_cols], na.rm = TRUE)
df$Confidence <- rowMeans(df[conf_cols], na.rm = TRUE)
df$CorrectnessPattern <- str_pad(as.character(df$CorrectnessPattern), 6, pad = "0")
df$Warm_Condition <- as.integer(df$Anthropomorphic)
df$EducationLevel <- as.integer(df$EducationLevel)

# 4. Detailed Question Logic
questions <- paste0("Q", 1:6)
for (q in questions) {
  # Clean specific columns (Fixes "type mismatch" error)
  target_cols <- c(paste0(q, "_Acc"), paste0(q, "_Confidence_1"), paste0(q, "_Familiarity"), paste0(q, "_Difficulty"), paste0("__js_", q, "_WebSearch_Touched"))
  df <- df %>% mutate(across(any_of(target_cols), ~ replace_na(suppressWarnings(as.integer(.)), 0)))
  
  # Rename WebSearch
  if(paste0("__js_", q, "_WebSearch_Touched") %in% names(df)) {
    df <- df %>% rename(!!paste0(q, "_WS_Touched") := paste0("__js_", q, "_WebSearch_Touched"))
  }
  
  # Timer
  timer_old <- paste0(q, "_Timer_Page Submit")
  if(timer_old %in% names(df)){
    df[[paste0(q, "_Timer_Page_Submit")]] <- as.numeric(df[[timer_old]])
  }
  
  # Correctness & Logic
  q_idx <- as.integer(substr(q, 2, 2))
  df[[paste0(q, "_Correctness")]] <- as.integer(str_sub(df$CorrectnessPattern, q_idx, q_idx))
  
  # URL Lists
  df[[paste0(q, "_URLs_Clicked_len")]] <- map_int(df[[paste0(q, "_URLs_Clicked")]], length)
  df[[paste0(q, "_Chatbot_Clicks")]] <- map_int(df[[paste0(q, "_URLs_Clicked")]], function(urls) {
    if (length(urls) == 0) return(0)
    sum(sapply(urls, is_chatbot_url))
  })
  
  # Chatbot Same
  cond1 <- (df[[paste0(q, "_Correctness")]] == 1) & (df[[q]] == "1")
  cond2 <- (df[[paste0(q, "_Correctness")]] == 0) & (df[[q]] == "2")
  df[[paste0(q, "_Chatbot_Same")]] <- as.integer(cond1 | cond2)
}

# 5. Conversations Processing
print("... Processing Conversations")
process_conversations <- function(response_id) {
  ret <- list(interactions = vector("list", 6), seen_ord = vector("list", 6))
  names(ret$interactions) <- paste0("Q", 1:6)
  names(ret$seen_ord) <- paste0("Q", 1:6)
  
  json_path <- file.path("../Conversations", paste0(tolower(response_id), ".jsonl"))
  if (file.exists(json_path)) {
    lines <- readLines(json_path, warn = FALSE)
    data <- map(lines, ~ fromJSON(., flatten = TRUE))
    
    session_starts <- list()
    for (entry in data) {
      if (entry$q != "Q0") {
        if (entry$kind == "chat_user") ret$interactions[[entry$q]] <- append(ret$interactions[[entry$q]], list(entry))
        if (entry$kind == "session_start") session_starts <- append(session_starts, list(entry))
      }
    }
    
    # Sort and Order
    get_ts <- function(x) ymd_hms(x$ts)
    if(length(session_starts) > 0){
      session_starts <- session_starts[order(sapply(session_starts, get_ts))]
      sorted_questions <- sapply(session_starts, function(x) x$q)
      ret$First_Q <- sorted_questions[1]
      for (j in seq_along(sorted_questions)) ret$seen_ord[[sorted_questions[j]]] <- j
    }
  }
  return(ret)
}

df$Init_Correctness <- NA_integer_
for(q in questions) df[[paste0(q, "_Interactions_len")]] <- 0
for(q in questions) df[[paste0(q, "_Seen_Order")]] <- NA_integer_

for (i in seq_len(nrow(df))) {
  res <- process_conversations(df$ResponseID[i])
  if (!is.null(res$First_Q)) {
    df$Init_Correctness[i] <- df[[paste0(res$First_Q, "_Correctness")]][i]
    for (q in questions) {
      df[[paste0(q, "_Interactions_len")]][i] <- length(res$interactions[[q]])
      if (!is.null(res$seen_ord[[q]])) df[[paste0(q, "_Seen_Order")]][i] <- res$seen_ord[[q]]
    }
  }
}

# 6. Filtering & Merging Prolific (Fixed Deduplication)
df$Interactions_Total <- rowMeans(df[paste0("Q", 1:6, "_Interactions_len")], na.rm=TRUE)
df$URLs_Clicked_Total <- rowMeans(df[paste0("Q", 1:6, "_URLs_Clicked_len")], na.rm=TRUE)

mask_duration <- df$`Duration (in seconds)` < 300
mask_same <- apply(df[paste0("Q", 1:6, "_Chatbot_Same")], 1, function(x) n_distinct(x) == 1)
df <- df[!(mask_duration & mask_same), ]

conf_cols <- paste0("Q", 1:6, "_Confidence_1")
diff_cols <- paste0("Q", 1:6, "_Difficulty")
fam_cols  <- paste0("Q", 1:6, "_Familiarity")

df$Conf_Std <- apply(df[conf_cols], 1, sd, na.rm = TRUE)
df$Diff_Std <- apply(df[diff_cols], 1, sd, na.rm = TRUE)
df$Fam_Std  <- apply(df[fam_cols],  1, sd, na.rm = TRUE)

df <- df[!(df$Diff_Std < 1 &
             df$Fam_Std  < 1 &
             df$Conf_Std < 1 &
             df$Interactions_Total < 1 &
             df$URLs_Clicked_Total < 1), ]

# Load and Deduplicate Prolific Data
prolific <- read_csv("../Prolific.csv", show_col_types = FALSE) %>% filter(Status == "APPROVED")
prolific_female <- read_csv("../Prolific_Female.csv", show_col_types = FALSE)
prolific_all <- bind_rows(prolific, prolific_female)

# Keep only one row per Participant ID
prolific_unique <- prolific_all %>% distinct(`Participant id`, .keep_all = TRUE)

df <- df %>%
  inner_join(prolific_unique, by = c("PID" = "Participant id"))

# Ensure StartDate is parsed as POSIX (format: "2025-11-11 15:00:49")
df <- df %>%
  mutate(StartDate_parsed = lubridate::ymd_hms(StartDate)) %>%
  arrange(PID, StartDate_parsed) %>%
  group_by(PID) %>%
  slice(1) %>%              # keep first (earliest) row per PID
  ungroup() %>%
  select(-StartDate_parsed)

# Binning
df$Age <- as.integer(df$Age)
df$AgeGroup <- cut(df$Age, breaks = c(18, 30, 40, 50, 60, 120), labels = c("19-30", "30-40", "40-50", "50-60", "60+"))
df$EducationLevelMapped <- recode(as.character(df$EducationLevel), "1"="HighSchool-", "2"="HighSchool-", "3"="Vocational", "4"="BA/BS", "5"="MA/MS+", "6"="MA/MS+")
df$UF_Mapped <- recode(as.character(df$UF_Q1), "1"="3+ times a day", "2"="1-2 times a day", "3"="Weekly", "4"="Monthly+", "5"="Monthly+", "6"="Monthly+", "7"="Monthly+")

print(paste("Data Processing Complete. Final Sample Size:", nrow(df)))

# ==============================================================================
# PART 3: MULTILEVEL ANALYSIS (lme4)
# ==============================================================================

print("... Starting Analysis")

# Reshape Wide to Long
df_long <- df %>%
  select(PID, DL, EducationLevelMapped, Age, AgeGroup, Trust, Warm_Condition, UF_Q1, UF_Mapped, Init_Correctness, CorrectnessPattern, matches("^Q[1-6]_")) %>%
  pivot_longer(cols = matches("^Q[1-6]_"), names_to = c("Question", ".value"), names_pattern = "Q(.)_(.*)") %>%
  rename(Confidence = Confidence_1, Accuracy = Acc) %>%
  mutate(Question = paste0("Q", Question))

# Factor conversion
cols_to_factor <- c("PID", "Question", "CorrectnessPattern", "Warm_Condition", "EducationLevelMapped", "AgeGroup", "UF_Mapped", "UF_Q1")
df_long <- df_long %>% mutate(across(all_of(cols_to_factor), as.factor))

variables <- c("Warm_Condition", "Warm_Condition:Interactions_len", "Warm_Condition:Chatbot_Clicks", "Confidence", "Correctness", "UF_Mapped", "UF_Mapped:Trust", "Difficulty", "Familiarity", "Trust", "DL", "Init_Correctness", "Interactions_len", "URLs_Clicked_len", "Seen_Order", "Chatbot_Clicks", "EducationLevelMapped", "AgeGroup")

# --- MODEL 1: ACCURACY (GLMM) ---
print("--- GEE Accuracy Prediction (Equivalent GLMM) ---")

f1 <- as.formula(
  paste("Accuracy ~", paste(variables, collapse = " + "), "+ (1|PID) + (1|Question)")
)

model_acc <- glmer(
  f1,
  data   = df_long,
  family = binomial,
  control = glmerControl(optimizer = "bobyqa")
)
print(summary(model_acc))

# --- MODEL 2: CONFIDENCE (LMM) ---
print("--- Confidence Prediction ---")
non_confidence <- setdiff(variables, "Confidence")
f2 <- as.formula(paste("Confidence ~", paste(non_confidence, collapse=" + "), "+ (1|PID) + (1|Question)"))
try({
  model_conf <- lmer(f2, data = df_long)
  print(summary(model_conf))
})

# --- MODEL 3: CHATBOT SAME (GLMM) ---
print("--- Chatbot Same Prediction ---")

f3 <- as.formula(
  paste("Chatbot_Same ~", paste(variables, collapse=" + "),
        "+ (1|PID) + (1|Question)")
)

model_chatbot <- glmer(
  f3,
  data   = df_long,
  family = binomial,
  control = glmerControl(optimizer = "bobyqa")
)
print(summary(model_chatbot))

# --- MODEL 4: URLS CLICKED (LMM) ---
print("--- URLs Clicked Prediction ---")
non_urls <- setdiff(variables, "URLs_Clicked_len")
f4 <- as.formula(paste("URLs_Clicked_len ~", paste(non_urls, collapse=" + "), "+ (1|PID) + (1|Question)"))
try({
  model_urls <- lmer(f4, data = df_long)
  print(summary(model_urls))
})

# --- MODEL 5: INTERACTIONS (LMM) ---
print("--- Interactions Prediction ---")
non_interact <- setdiff(variables, c("Interactions_len", "Warm_Condition:Interactions_len"))
f5 <- as.formula(paste("Interactions_len ~", paste(non_interact, collapse=" + "), "+ (1|PID) + (1|Question)"))
try({
  model_interact <- lmer(f5, data = df_long)
  print(summary(model_interact))
})

print("Analysis Complete.")

# ------------------------------------------ #
# Power Analysis
# ------------------------------------------ #

# --- MixedPower ---

df_long$PID_num <- as.numeric(as.factor(df_long$PID))
df_long$Q_num <- as.numeric(as.factor(df_long$Question))
fixed_effects <- c(
  "Chatbot_Clicks",
  "Warm_Condition",
  "UF_Mapped",
  "AgeGroup"
)

steps <- c(50, 100, 150, 200, 250) 
model <- model_acc
# steps <- c(100) 
critical_value <- 2
n_sim <- 25
beta_hat <- fixef(model)
SESOI <- as.numeric(beta_hat) * 0.85
set.seed(123)

power_acc <- mixedpower(
  model          = model,
  data           = df_long,
  fixed_effects  = fixed_effects,
  simvar         = "PID_num",
  steps          = steps,
  critical_value = critical_value,
  n_sim          = n_sim,
  # SESOI          = SESOI,
)

power_acc        

multiplotPower(power_acc)

# --- SimR ---

fixef(model_acc)

set.seed(123)
powerC <- powerCurve(fit = model, test = fixed("Warm_Condition"), along = "PID",
                     breaks = steps, nsim=10)
plot(p_curve_treat)
