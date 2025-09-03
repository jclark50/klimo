
# ============================== Bias-correcting pipeline (numeric-only IO) ==============================
suppressPackageStartupMessages({
  library(data.table)
  library(aws.s3)
  library(lubridate)
  library(jj)
  library(dplyr)
  library(arrow)
  library(terra)
  library(geosphere)
  library(RANN)
  library(geodist)
  library(DBI)
  
})


source("/opt/klimo/code/sentry_utils.R")

timed('start')



list_all_s3_objects <- function(bucket,
                                prefix       = NULL,
                                fileext      = NULL,
                                returnTail   = FALSE,
                                max_keys     = 10000)
{
  if (!requireNamespace("aws.s3", quietly = TRUE)) {
    stop("Please install the 'aws.s3' package to use list_all_s3_objects().")
  }
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("Please install the 'data.table' package to use list_all_s3_objects().")
  }
  
  # Ensure prefix ends with “/” if not NULL
  if (!is.null(prefix) && !grepl("/$", prefix)) {
    prefix <- paste0(prefix, "/")
  }
  
  all_pages <- list()   # we will append each page’s DT here
  marker    <- NULL     # for the first page, marker=NULL
  
  repeat {
    # Build the argument list for get_bucket()
    args <- list(
      bucket = bucket,
      prefix = prefix,
      max    = max_keys
    )
    if (!is.null(marker)) {
      args$marker <- marker
    }
    
    # Fetch one “page” of up to max_keys objects
    this_batch <- try(do.call(aws.s3::get_bucket, args), silent = TRUE)
    if (inherits(this_batch, "try-error")) {
      warning("Error fetching from S3; returning what we have so far.")
      break
    }
    if (length(this_batch) == 0) {
      # No more objects under that prefix
      break
    }
    
    # Parse each object’s metadata into a list of lists
    page_list <- lapply(this_batch, function(obj) {
      list(
        Key          = obj$Key,
        LastModified = obj$LastModified,
        ETag         = obj$ETag,
        Size         = obj$Size,
        # Owner        = if (!is.null(obj$Owner$DisplayName)) obj$Owner$DisplayName else NA_character_,
        StorageClass = obj$StorageClass
      )
    })
    
    # rbind into a data.table
    page_dt <- data.table::rbindlist(page_list, fill = TRUE)
    all_pages[[length(all_pages) + 1]] <- page_dt
    
    # If fewer than max_keys returned, we’re done
    if (length(this_batch) < max_keys) {
      break
    }
    
    # Otherwise set marker = last key from this batch, to fetch the next batch
    marker <- tail(this_batch, n = 1)[[1]]$Key
  }
  
  # If nothing was fetched, return NULL
  if (length(all_pages) == 0) {
    message(sprintf("No objects found in s3://%s/%s", bucket, prefix))
    return(NULL)
  }
  
  # Combine all pages into one data.table
  all_dt <- data.table::rbindlist(all_pages, use.names = TRUE, fill = TRUE)
  
  # Convert LastModified → POSIXct (UTC) if it's still character
  if (is.character(all_dt$LastModified)) {
    all_dt[, LastModified := as.POSIXct(
      LastModified,
      format = "%Y-%m-%dT%H:%M:%OSZ",
      tz     = "UTC"
    )]
  }
  
  # Optionally filter by file extension/pattern
  if (!is.null(fileext)) {
    all_dt <- all_dt[grepl(fileext, Key, ignore.case = TRUE), ]
  }
  
  # Sort ascending by LastModified
  data.table::setorder(all_dt, LastModified)
  
  # Optionally return only the newest rows
  if (returnTail) {
    return(tail(all_dt, n = max_keys))
  }
  
  # Drop any “folder” placeholder rows equal to exactly the prefix (if they exist)
  if (!is.null(prefix)) {
    all_dt <- all_dt[Key != prefix]
  }
  
  return(all_dt)
}

##############################################################
unloadNamespace('RMySQL')
library(DBI)


conn <- dbConnect(
  RMariaDB::MariaDB(),
  user         = "jordan",
  password     = "wxther50!!@@",
  dbname       = "klimoWBGT",
  host         = "44.197.87.201",
  port         = 3306,
  local_infile = TRUE # required for LOAD DATA LOCAL INFILE
)
on.exit(try(DBI::dbDisconnect(conn), silent = TRUE), add = TRUE)

info_dt = as.data.table(dbGetQuery(conn, "SELECT * FROM klimoWBGT.info"))

# 
# 
# sqlquer = 
#   "
#   WITH base AS (
#   SELECT
#     i.siteRecID AS id,
#     i.tz,
#     CASE
#       WHEN CONVERT_TZ('2000-01-01 00:00:00','UTC', i.tz) IS NULL
#         THEN UTC_TIMESTAMP()
#       ELSE CONVERT_TZ(UTC_TIMESTAMP(),'UTC', i.tz)
#     END AS local_now
#   FROM klimoWBGT.info i
# ),
# bounds AS (
#   SELECT
#     id, tz,
#     DATE(local_now) AS local_date,
#     /* start/end of local day, converted back to UTC; if tz unusable, they’re already UTC */
#     CASE
#       WHEN CONVERT_TZ('2000-01-01 00:00:00','UTC', tz) IS NULL
#         THEN DATE(local_now)
#       ELSE CONVERT_TZ(DATE(local_now), tz, 'UTC')
#     END AS start_utc,
#     CASE
#       WHEN CONVERT_TZ('2000-01-01 00:00:00','UTC', tz) IS NULL
#         THEN DATE(local_now) + INTERVAL 1 DAY
#       ELSE CONVERT_TZ(DATE(local_now) + INTERVAL 1 DAY, tz, 'UTC')
#     END AS end_utc
#   FROM base
# )
# SELECT
#   b.id,
#   b.tz,
#   b.local_date,
#   COALESCE(SUM(COALESCE(w.ms_qpe_01h_p2_mm, w.qpe01h_mm)), 0) AS precip_mm_today,
#   COUNT(COALESCE(w.ms_qpe_01h_p2_mm, w.qpe01h_mm))            AS hours_counted
# FROM bounds b
# LEFT JOIN klimoWBGT.mrms_point_extracts_wide w
#   ON w.id = b.id
#  AND w.ts_utc >= b.start_utc
#  AND w.ts_utc <  b.end_utc
# GROUP BY b.id, b.tz, b.local_date
# ORDER BY b.id;
# 
# "
# fdfd = as.data.table(dbGetQuery(conn, sqlquer))
# 
# fdfd[id == '1423c879-d15b-4083-9b73-3a4940fea1ee']
# 
# 
# fdfd$precip_in = round(fdfd$precip_mm_today / 25.4, 2)
# 
# fdfd[precip_in  > 0]
# 
# 
# # 
# # fdfd = as.data.table(dbGetQuery(conn, 'select * from mrms_point_extracts_wide'))
# # fdfd$ms_qpe_24h_p1_in = round(fdfd$ms_qpe_24h_p1_mm / 25.4, 2)
# # fdfd$ms_qpe_24h_p2_in = round(fdfd$ms_qpe_24h_p2_mm / 25.4, 2)
# # 
# # # # max(fdfd$ms_qpe_24h_p1_in, na.rm=TRUE)
# # # max(fdfd$ms_qpe_24h_p2_in, na.rm=TRUE)
# # fdfd$rain_24h = fdfd$ms_qpe_24h_p2_in
# # fdfd = fdfd[,c("id","rain_24h")]
# # fdfd[is.na(rain_24h), rain_24h := 0]
# # # 
# # # 
# # # 
# # # max(fdfd$qpe01h_mm, na.rm=TRUE)
# # # 
# # # fdfd[qpe01h_mm == max(fdfd$qpe01h_mm, na.rm=TRUE)]
# # # 
# # # 
# fdfd = as.data.table(dbGetQuery(conn, 'select * from mrms_site_latest'))
# fdfd$ms_qpe_24h_p1_in = round(fdfd$ms_qpe_24h_p1_mm / 25.4, 2)
# fdfd$ms_qpe_24h_p2_in = round(fdfd$ms_qpe_24h_p2_mm / 25.4, 2)
# 
# # # max(fdfd$ms_qpe_24h_p1_in, na.rm=TRUE)
# # max(fdfd$ms_qpe_24h_p2_in, na.rm=TRUE)
# fdfd$rain_24h = fdfd$ms_qpe_24h_p2_in
# fdfd = fdfd[,c("id","rain_24h")]
# fdfd[is.na(rain_24h), rain_24h := 0]
# 
# 
# fdfd[order(rain_24h)]







################################################################################
################################################################################



# ─── helper to block until RAM is available ────────────────────────────────
requireRAM <- function(threshold = 4000,
                       unit = "MB",
                       wait_time = 10,
                       max_attempts = 10) {
  
  #' Check Available System RAM
  check_ram <- function(threshold,
                        unit = "MB",
                        wait_time = 60,
                        max_attempts = 3) {
    # Convert threshold into bytes
    multiplier <- switch(toupper(unit),
                         "B"  = 1,
                         "KB" = 1024,
                         "MB" = 1024^2,
                         "GB" = 1024^3,
                         stop("Invalid unit. Use B, KB, MB, or GB."))
    threshold_bytes <- threshold * multiplier
    
    # Internal helper to get free RAM in bytes
    get_available_ram <- function() {
      if (.Platform$OS.type == "windows") {
        mem_info <- system2("wmic",
                            args = c("OS", "get", "FreePhysicalMemory", "/Value"),
                            stdout = TRUE)
        # Parse "FreePhysicalMemory=xxxx"
        value_line <- grep("FreePhysicalMemory", mem_info, value = TRUE)
        kb <- as.numeric(sub("FreePhysicalMemory=", "", value_line))
        return(kb * 1024)
      } else {
        mem_info <- system2("free", args = c("-b"), stdout = TRUE)
        # The 7th column of the second line is "available" on Linux
        parts <- strsplit(mem_info[2], "\\s+")[[1]]
        as.numeric(parts[7])
      }
    }
    
    for (attempt in seq_len(max_attempts)) {
      available_bytes <- get_available_ram()
      available <- available_bytes / multiplier
      
      if (available_bytes >= threshold_bytes) {
        cat(sprintf("Attempt %d: Available RAM = %s %s >= threshold %s %s\n",
                    attempt,
                    format(available, big.mark = ","),
                    unit,
                    threshold,
                    unit))
        return(TRUE)
      }
      
      cat(sprintf("Attempt %d: Available RAM = %s %s < threshold %s %s; \
        waiting %s seconds...\n",
                  attempt,
                  format(available, big.mark = ","),
                  unit,
                  threshold,
                  unit,
                  wait_time))
      Sys.sleep(wait_time)
    }
    
    cat(sprintf("RAM availability remained below %s %s after %d attempts. Exiting.\n",
                threshold, unit, max_attempts))
    quit(save = "no", status = 1)
  }
  
  
  
  attempts <- 0
  while (!check_ram(threshold,
                    unit = unit,
                    wait_time = wait_time,
                    max_attempts = 1)) {
    attempts <- attempts + 1
    if (attempts >= max_attempts) {
      stop(sprintf("Available RAM did not reach %s %s after %d attempts",
                   threshold, unit, max_attempts))
    }
    message(sprintf("Retry #%d: waiting for ≥%s %s free…",
                    attempts, threshold, unit))
    Sys.sleep(wait_time)
  }
  invisible(TRUE)
}


requireRAM(3000, wait_time = 4, max_attempts = 20)   # blocks until you have ≥4 GB free




################################################################################
################################################################################

get_xano_table <- function(table   = 'sites',
                           query     = list(),
                           auth_tok  = NULL) {
  
  if (table == 'sites'){
    get_url = 'https://xd8o-zhxg-pkco.n7e.xano.io/api:TkOVqxmz/jordan/wbgt_sites'
  } else if (table == 'alerts') {
    get_url = 'https://xd8o-zhxg-pkco.n7e.xano.io/api:TkOVqxmz/wbgt_alerts_nws'
  }
  
  headers <- c(`Content-Type` = "application/json")
  if (!is.null(auth_tok)) {
    headers["Authorization"] <- paste("Bearer", auth_tok)
  }
  
  resp <- httr::GET(
    url    = get_url,
    query  = query,
    httr::add_headers(.headers = headers)
  )
  
  httr::stop_for_status(resp)
  outdt = as.data.table(httr::content(resp, "parsed", simplifyVector = TRUE))
  
  if (table == 'sites'){
    
    rename.cols(outdt, "id", "siteRecID")
    
  }
  
  return(outdt)
  
}

xanoSites = get_xano_table('sites')

# ─── 2) Configuration ─────────────────────────────────────────────────────────
# bucket     <- "klimo-forecastmodels"
bucket     <- 'klimo-current'
prefix     <- "current_grid/"
save_dir   <- "/data"
save_file  <- file.path("/data/current_wbgt_grid.parquet")

# ─── 3) Ensure target directory exists ────────────────────────────────────────
dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)

full_ds     <- try(open_dataset(save_file,
                                thrift_string_size_limit    = 1e9,
                                thrift_container_size_limit = 1e8))



# names(full_ds)[order(names(full_ds))]



if (inherits(full_ds, "try-error")){
  file.remove(save_file)
  pausecode(6)
  success <- aws.s3::save_object(
    object = latest_key,
    bucket = bucket,
    file   = save_file
  )
  message("Downloaded to ", save_file)
  full_ds     <- try(open_dataset(save_file))
  
}



needed_cols <- c("lat","lon","index", "ta","relh","wind2m",'wind10m','WDIR',
                 "hi","refTime","validTime",'wind2m_shade', 'wind2m_sun',
                 "tcdc",'lcdc','mcdc','hcdc', "solar", 'solar_shade', 'solar_sun',
                 "REFC",
                 'CRAIN',"model",'uvi','uvi_est','uvi_clear'
                 # "wbgt", "wbgt_shade", "wbgt_sun"
)

names(full_ds)[order(names(full_ds))]


needed_cols_available = names(full_ds)[names(full_ds) %in% needed_cols]


dt_indexed  <- full_ds %>%
  select(all_of(needed_cols_available)) %>%
  # filter(index %in% info_dt$index) %>%  # Filter to only the indices we need
  collect() %>%
  as.data.table()

# Decide which models you want to process
models <- c("hrrr","hrrr_subh",'rtma_rapid') #,'rtma_co')

# We'll keep a list of results, one element per model
results_by_model <- vector("list", length(models))
names(results_by_model) <- models


# mdl = 'hrrr'

for (mdl in models) {
  dt_mod <- dt_indexed[model == mdl]
  
  # 2b) Build a small "grid lookup" table of unique (lat, lon, index) for this model
  grid_dt     <- unique(dt_mod[, .(lat, lon, index)])
  grid_lon    <- grid_dt$lon
  grid_lat    <- grid_dt$lat
  grid_idx    <- grid_dt$index
  
  # 2c) For each site in info_dt, find its nearest 'index' for this model
  #      We create a new column on info_dt called nearest_index_<mdl>
  nn_colname <- paste0("nearest_index_", mdl)
  info_dt[, (nn_colname) := {
    # 'lon' and 'lat' here refer to columns of info_dt (site coordinates)
    siteLon <- lon
    siteLat <- lat
    # Compute squared Euclidean distance in (lon, lat) space:
    d2 <- (grid_lon - siteLon)**2 + (grid_lat - siteLat)**2
    # which.min(d2) gives the row in grid_dt with smallest distance
    grid_idx[which.min(d2)]
  }, by = seq_len(nrow(info_dt))]
  
  # 2d) Now merge every siteRecID → dt_mod row via that nearest‐index
  #     We only keep sites that actually found a matching index (most should)
  cand <- merge(
    info_dt[, .(siteRecID, nearest_index = get(nn_colname))],  # left: (siteRecID, nearest_index)
    dt_mod,                                                     # right: everything for this model
    by.x  = "nearest_index",
    by.y  = "index",
    all.x = TRUE                                               # keep any siteRecID even if unmatched
  )
  dt_mod = ''; gc()
  # cand now has columns: nearest_index, siteRecID, lat, lon, index, model, wbgt, ta, …, validTime, etc.
  # (Note: index and nearest_index are identical, so you can drop one if you like.)
  
  # 2e) For each siteRecID, keep only the row with the **latest** validTime
  #     (That is, order by siteRecID and descending validTime, then take first per group.)
  setorder(cand, siteRecID, -validTime)
  row_latest_mdl <- cand # [, .SD[1], by = siteRecID]
  
  # 2f) Attach any additional site metadata you want (e.g. state, organization, etc.)
  #     Just merge by siteRecID back from info_dt
  row_latest_mdl <- merge(
    row_latest_mdl,
    info_dt[, .(siteRecID, policyGroupID)],
    by    = "siteRecID",
    all.x = TRUE
  )
  
  # 2g) Tag this block with the model name so we can stack later
  row_latest_mdl[, model := mdl]
  
  # 2h) Store in our results list
  results_by_model[[mdl]] <- row_latest_mdl
  
  grid_dt = ''
  rm(grid_dt)
  gc()
  
  
}

# ──────────
row_latest = rbindlist(results_by_model)

row_latest = row_latest[!is.na(ta)]

row_latest$td = calcDewpoint(row_latest$ta, row_latest$relh, 'degC', 'degC')


# row_latest[siteRecID == 'Sycaten']




############################################################--------------------------------
############################################################--------------------------------

############################################################--------------------------------
############################################################--------------------------------



# BIAS CORRECTION
# Added 20250821


############################################################--------------------------------
############################################################--------------------------------

############################################################--------------------------------
############################################################--------------------------------
# fetch_latest_biases <- function(conn, site_ids = NULL, table = "bias_latest_json") {
#   where <- if (length(site_ids)) {
#     sprintf("WHERE siteRecID IN (%s)", paste(sprintf("'%s'", site_ids), collapse = ","))
#   } else ""
#   dt <- data.table(DBI::dbGetQuery(conn, sprintf("SELECT * FROM %s %s", table, where)))
#   if (!nrow(dt)) return(dt)
# 
#   # expand JSON to columns
#   bj <- lapply(dt$biases_json, function(x) {
#     y <- tryCatch(jsonlite::fromJSON(x), error = function(e) list())
#     as.data.table(as.list(y))
#   })
#   biases <- data.table::rbindlist(bj, fill = TRUE)
#   dt[, biases_json := NULL]
#   cbind(dt, biases)
# }
# source("/opt/klimo/code/biasCorrectionFunctions.R")
# conn <- dbConnect(RMySQL::MySQL(),
#                   user = "jordan", password = "wxther50!!@@",
#                   dbname = 'klimoWBGT', host = Sys.getenv("SQL_SERVER_IP"),
#                   port = 3306, local_infile = TRUE)
# bias_latest <- fetch_latest_biases(conn)
# dbDisconnect(conn)
# row_latest_corrected  <- try(apply_biases(row_latest, bias_latest))
# if (inherits(row_latest_corrected, "try-error")){
#   row_latest_corrected = row_latest
# }
# 
# 
##############################################################
##############################################################
##############################################################
##############################################################
##############################################################
# unloadNamespace('RMySQL')
# library(DBI)

# therowlatest = row_latest
apply_biases2 = function(therowlatest){
  
  # unloadNamespace('RMySQL')
  # library(DBI)
  # 
  # conn <- dbConnect(RMySQL::MySQL(),
  #                   user = "jordan", password = "wxther50!!@@",
  #                   dbname = 'klimoWBGT', host = Sys.getenv("SQL_SERVER_IP"),
  #                   port = 3306, local_infile = TRUE)
  # 
  # 
  suppressPackageStartupMessages({
    library(data.table); library(DBI); library(RMySQL); library(jsonlite)
  })
  
  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
  .clamp   <- function(x, lo, hi) pmin(hi, pmax(lo, x))
  .to_frac <- function(x) { if (length(x)==0L) return(x); p<-ifelse(suppressWarnings(max(x,na.rm=TRUE))<=1.01,x,x/100); eps<-1e-6; pmin(1-eps,pmax(eps,p)) }
  .logit    <- function(p) log(p/(1-p))
  .invlogit <- function(x) 1/(1+exp(-x))
  
  models_to_apply <- c("hrrr", "hrrr_subh")
  
  # ---- caps (create cfg if missing, fill defaults) ----
  if (!exists("cfg") || is.null(cfg)) cfg <- list()
  cfg$cap_abs_dT_F        <- cfg$cap_abs_dT_F        %||% 6
  cfg$cap_abs_dTd_F       <- cfg$cap_abs_dTd_F       %||% 8
  cfg$cap_abs_dRH_pct     <- cfg$cap_abs_dRH_pct     %||% 20
  cfg$cap_wind_ratio_min  <- cfg$cap_wind_ratio_min  %||% 0.4
  cfg$cap_wind_ratio_max  <- cfg$cap_wind_ratio_max  %||% 3.5
  cfg$cap_solar_ratio_min <- cfg$cap_solar_ratio_min %||% 0.5
  cfg$cap_solar_ratio_max <- cfg$cap_solar_ratio_max %||% 1.5
  cfg$cap_abs_tcdc_pp     <- cfg$cap_abs_tcdc_pp     %||% 30
  cfg$cap_abs_tcdc_logit  <- cfg$cap_abs_tcdc_logit  %||% 3
  
  # ---- the *_bias column names we expect from SQL JSON (DO NOT CHANGE HERE) ----
  bias_colnames_apply <- c(
    "ta_bias","td_bias","rh_bias",
    "wind10m_bias","solar_bias",
    "tcdc_pp_bias","tcdc_logit_bias",
    "dLCDC_bias","dMCDC_bias","dHCDC_bias"
  )
  # 
  # stopifnot(exists("row_latest"), nrow(row_latest) > 0)
  # 
  # unloadNamespace('RMySQL')
  # library(DBI)
  # 
  # # ---------- 1) FETCH & EXPAND LATEST BIASES FROM SQL ----------
  # if (!exists("conn") || is.null(conn)) {
  #   conn <- dbConnect(RMySQL::MySQL(),
  #                     user = "jordan",
  #                     password = "wxther50!!@@",
  #                     dbname = "klimoWBGT",
  #                     host = Sys.getenv("SQL_SERVER_IP"),
  #                     port = 3306,
  #                     local_infile = TRUE)
  #   on.exit(try(dbDisconnect(conn), silent = TRUE), add = TRUE)
  # }
  # 
  site_ids <- unique(row_latest$siteRecID)
  where <- if (length(site_ids)) {
    sprintf("WHERE siteRecID IN (%s)", paste(sprintf("'%s'", site_ids), collapse = ","))
  } else ""
  
  bias_table <- if (nzchar(Sys.getenv("BIAS_TABLE"))) Sys.getenv("BIAS_TABLE") else "bias_latest_json2"
  sql <- sprintf(
    "SELECT siteRecID, source, validTime, created_at, biases_json, age_min_src, obs_age_min
   FROM %s %s", bias_table, where)
  
  dt <- data.table(DBI::dbGetQuery(conn, sql))
  if (!nrow(dt)) stop("No rows returned from ", bias_table, " for these sites.")
  
  bias_json_expanded <- rbindlist(
    lapply(dt$biases_json, function(js) {
      out <- tryCatch(as.list(jsonlite::fromJSON(js)), error = function(e) list())
      for (nm in names(out)) out[[nm]] <- suppressWarnings(as.numeric(out[[nm]]))
      as.data.table(out)
    }),
    fill = TRUE
  )
  
  bias_latest <- cbind(
    dt[, .(siteRecID, source, validTime, created_at, age_min_src, obs_age_min)],
    bias_json_expanded
  )
  
  # Ensure all expected *_bias columns exist
  for (nm in bias_colnames_apply) if (!nm %in% names(bias_latest)) bias_latest[, (nm) := NA_real_]
  
  # ---------- 2) MERGE & APPLY TO CURRENT ROWS ----------
  rows_to_apply <- copy(therowlatest[model %in% models_to_apply])
  rows_keep     <- copy(therowlatest[!model %in% models_to_apply])
  
  rows_to_apply <- merge(
    rows_to_apply,
    bias_latest[, c("siteRecID", bias_colnames_apply), with = FALSE],
    by = "siteRecID",
    all.x = TRUE
  )
  
  # Neutral replacements for missing bias values
  nz_add <- function(x) { x <- suppressWarnings(as.numeric(x)); ifelse(is.finite(x), x, 0) }
  nz_mul <- function(x) { x <- suppressWarnings(as.numeric(x)); ifelse(is.finite(x), x, 1) }
  
  # Temperatures (°C inputs)
  if ("ta" %in% names(rows_to_apply)) {
    rows_to_apply[, ta_bc := ta + nz_add(ta_bias)]
    rows_to_apply[, ta_bc := .clamp(ta_bc,
                                    ta - (cfg$cap_abs_dT_F * 5/9),
                                    ta + (cfg$cap_abs_dT_F * 5/9))]
  }
  if ("td" %in% names(rows_to_apply)) {
    rows_to_apply[, td_bc := td + nz_add(td_bias)]
    rows_to_apply[, td_bc := .clamp(td_bc,
                                    td - (cfg$cap_abs_dTd_F * 5/9),
                                    td + (cfg$cap_abs_dTd_F * 5/9))]
  }
  
  # Relative humidity (% points)
  if ("relh" %in% names(rows_to_apply)) {
    rows_to_apply[, relh_bc := .clamp(relh + nz_add(rh_bias), 0, 100)]
  }
  
  # Wind ratios
  if ("wind10m" %in% names(rows_to_apply)) {
    wr <- nz_mul(rows_to_apply$wind10m_bias)
    wr <- .clamp(wr, cfg$cap_wind_ratio_min, cfg$cap_wind_ratio_max)
    rows_to_apply[, wind10m_bc := pmax(0, wind10m * wr)]
    if ("wind2m" %in% names(rows_to_apply))       rows_to_apply[, wind2m_bc       := pmax(0, wind2m       * wr)]
    if ("wind2m_shade" %in% names(rows_to_apply)) rows_to_apply[, wind2m_shade_bc := pmax(0, wind2m_shade * wr)]
    if ("wind2m_sun" %in% names(rows_to_apply))   rows_to_apply[, wind2m_sun_bc   := pmax(0, wind2m_sun   * wr)]
  }
  
  # Solar ratios
  if ("solar" %in% names(rows_to_apply)) {
    sr <- nz_mul(rows_to_apply$solar_bias)
    sr <- .clamp(sr, cfg$cap_solar_ratio_min, cfg$cap_solar_ratio_max)
    rows_to_apply[, solar_bc := pmax(0, as.numeric(solar) * sr)]
    if ("solar_shade" %in% names(rows_to_apply)) rows_to_apply[, solar_shade_bc := pmax(0, as.numeric(solar_shade) * sr)]
    if ("solar_sun" %in% names(rows_to_apply))   rows_to_apply[, solar_sun_bc   := pmax(0, as.numeric(solar_sun)   * sr)]
  }
  
  # Clouds (prefer logit; fallback to %-point)
  if ("tcdc" %in% names(rows_to_apply)) {
    tcdc_logit_b <- pmin(cfg$cap_abs_tcdc_logit, pmax(-cfg$cap_abs_tcdc_logit, nz_add(rows_to_apply$tcdc_logit_bias)))
    tcdc_pp_b    <- pmin(cfg$cap_abs_tcdc_pp,    pmax(-cfg$cap_abs_tcdc_pp,    nz_add(rows_to_apply$tcdc_pp_bias)))
    rows_to_apply[, tcdc_bc := tcdc]
    use_logit <- is.finite(tcdc_logit_b) & (abs(tcdc_logit_b) > 0)
    
    
    rows_to_apply[, tcdc_logit_b := pmin(cfg$cap_abs_tcdc_logit,
                                         pmax(-cfg$cap_abs_tcdc_logit, nz_add(tcdc_logit_bias)))]
    
    rows_to_apply[, tcdc_pp_b := pmin(cfg$cap_abs_tcdc_pp,
                                      pmax(-cfg$cap_abs_tcdc_pp, nz_add(tcdc_pp_bias)))]
    
    rows_to_apply[, tcdc_bc := tcdc]
    
    rows_to_apply[ is.finite(tcdc_logit_b) & tcdc_logit_b != 0,
                   tcdc_bc := .invlogit(.logit(.to_frac(as.numeric(tcdc))) + tcdc_logit_b) * 100
    ]
    
    rows_to_apply[ !(is.finite(tcdc_logit_b) & tcdc_logit_b != 0),
                   tcdc_bc := .clamp(as.numeric(tcdc) + tcdc_pp_b, 0, 100)
    ]
  }
  if ("lcdc" %in% names(rows_to_apply)) rows_to_apply[, lcdc_bc := .clamp(as.numeric(lcdc) + nz_add(dLCDC_bias), 0, 100)]
  if ("mcdc" %in% names(rows_to_apply)) rows_to_apply[, mcdc_bc := .clamp(as.numeric(mcdc) + nz_add(dMCDC_bias), 0, 100)]
  if ("hcdc" %in% names(rows_to_apply)) rows_to_apply[, hcdc_bc := .clamp(as.numeric(hcdc) + nz_add(dHCDC_bias), 0, 100)]
  
  # Recombine corrected + untouched models
  row_latest_bc <- rbindlist(list(rows_to_apply, rows_keep), use.names = TRUE, fill = TRUE)
  
  # ---------- 3) DIAGNOSTICS ----------
  message("Applied biases to: ", nrow(rows_to_apply), " rows across models: ",
          paste(sort(unique(rows_to_apply$model)), collapse = ", "))
  
  have_bias <- rows_to_apply[, .(
    ta_bias = sum(is.finite(ta_bias)), td_bias = sum(is.finite(td_bias)),
    rh_bias = sum(is.finite(rh_bias)), wind10m_bias = sum(is.finite(wind10m_bias)),
    solar_bias = sum(is.finite(solar_bias)), tcdc_logit_bias = sum(is.finite(tcdc_logit_bias)),
    tcdc_pp_bias = sum(is.finite(tcdc_pp_bias)), dLCDC_bias = sum(is.finite(dLCDC_bias)),
    dMCDC_bias = sum(is.finite(dMCDC_bias)), dHCDC_bias = sum(is.finite(dHCDC_bias))
  )]
  print(have_bias)
  
  # Spot check one site if present
  if ("Sycaten" %in% therowlatest$siteRecID) {
    cat("\n--- Sycaten BEFORE ---\n")
    print(therowlatest[siteRecID=="Sycaten", .(validTime, ta, td, relh, wind10m, solar, tcdc)][1])
    cat("\n--- Sycaten AFTER (bias-corrected) ---\n")
    print(row_latest_bc[siteRecID=="Sycaten", .(validTime, ta_bc, td_bc, relh_bc, wind10m_bc, solar_bc, tcdc_bc)][1])
  }
  
  
  
  
  # models_to_apply <- c("hrrr","hrrr_subh")
  # 
  # cols_additive <- c("ta","td","relh","tcdc","lcdc","mcdc","hcdc")
  # cols_ratio    <- c("wind10m","wind2m","wind2m_shade","wind2m_sun",
  #                    "solar","solar_shade","solar_sun")
  # 
  # # (Optional, recommended) recompute dewpoint from corrected Ta/RH for physical consistency
  # # recompute_td_from_ta_rh <- TRUE
  # 
  # # Replace base columns with *_bc when available and finite
  # overwrite_from_bc <- function(dt, cols, models = models_to_apply) {
  #   for (c in cols) {
  #     bc <- paste0(c, "_bc")
  #     if (bc %in% names(dt)) {
  #       dt[model %in% models & is.finite(get(bc)), (c) := get(bc)]
  #     }
  #   }
  #   invisible(dt)
  # }
  # 
  # # Apply
  # overwrite_from_bc(row_latest_bc, c(cols_additive, cols_ratio))
  # 
  # # # Consistent Td: prefer recompute from ta/relh after overwriting, else fall back to td_bc if present
  # # if (recompute_td_from_ta_rh && all(c("ta","relh") %in% names(row_latest_bc))) {
  # #   row_latest_bc[model %in% models_to_apply,
  # #                 td := calcDewpoint(ta, relh, 'degC','degC')]
  # # } else if ("td_bc" %in% names(row_latest_bc)) {
  # #   row_latest_bc[model %in% models_to_apply & is.finite(td_bc), td := td_bc]
  # # }
  # # 
  # # # (Optional) drop the *_bc columns if you don’t want to carry them
  # # keep <- setdiff(names(row_latest_bc), grep("_bc$", names(row_latest_bc), value=TRUE))
  # # row_latest_bc <- row_latest_bc[, ..keep]
  # # 
  # # 
  
  # --- after you have row_latest_bc with *_bc columns ---
  
  # Which models to overwrite in-place:
  models_to_apply <- c("hrrr","hrrr_subh")
  
  # Base columns we might overwrite from their *_bc counterparts
  cols_additive <- c("ta","td","relh","tcdc","lcdc","mcdc","hcdc")
  cols_ratio    <- c("wind10m","wind2m","wind2m_shade","wind2m_sun",
                     "solar","solar_shade","solar_sun")
  cols_all      <- intersect(c(cols_additive, cols_ratio), names(row_latest_bc))
  
  # 1) Make sure target columns are numeric (double) so no truncation happens
  #    (Parquet often brings solar columns in as integer.)
  cols_int <- cols_all[vapply(row_latest_bc[, ..cols_all], is.integer, logical(1))]
  if (length(cols_int)) {
    row_latest_bc[, (cols_int) := lapply(.SD, as.numeric), .SDcols = cols_int]
  }
  
  # 2) Overwrite base columns with *_bc where finite and for the chosen models
  overwrite_from_bc <- function(dt, cols, models = c("hrrr","hrrr_subh")) {
    for (c in cols) {
      bc <- paste0(c, "_bc")
      if (bc %in% names(dt)) {
        # ensure the RHS type matches the LHS type (paranoia; should be double now)
        rhs <- dt[[bc]]
        # only overwrite rows where we actually have a finite correction and model matches
        idx <- dt$model %in% models & is.finite(rhs)
        if (any(idx, na.rm = TRUE)) {
          # assign by reference
          set(dt, i = which(idx), j = c, value = rhs[idx])
        }
      }
    }
    invisible(dt)
  }
  
  overwrite_from_bc(row_latest_bc, cols_all, models_to_apply)
  
  # (Optional) recompute dewpoint for physical consistency
  if (all(c("ta","relh") %in% names(row_latest_bc)) && "td" %in% names(row_latest_bc)) {
    # replace with your own dewpoint function if different
    row_latest_bc[model %in% models_to_apply & is.finite(ta) & is.finite(relh),
                  td := calcDewpoint(ta, relh, 'degC', 'degC')]
  }
  
  # 3) (Optional) Drop *_bc helper columns once applied
  drop_bc <- TRUE
  if (drop_bc) {
    bc_to_drop <- paste0(cols_all, "_bc")
    bc_to_drop <- intersect(bc_to_drop, names(row_latest_bc))
    if (length(bc_to_drop)) row_latest_bc[, (bc_to_drop) := NULL]
  }
  
  # # 4) Quick sanity: how many values changed?
  # changed_summary <- row_latest_bc[model %in% models_to_apply,
  #                                  .(
  #                                    n_ta    = sum(!is.na(get("ta_bc"))    & abs(get("ta_bc")    - ta)    > 1e-9, na.rm=TRUE),
  #                                    n_td    = sum(!is.na(get("td_bc"))    & abs(get("td_bc")    - td)    > 1e-9, na.rm=TRUE),
  #                                    n_relh  = sum(!is.na(get("relh_bc"))  & abs(get("relh_bc")  - relh)  > 1e-9, na.rm=TRUE),
  #                                    n_w10   = sum(!is.na(get("wind10m_bc")) & abs(get("wind10m_bc") - wind10m) > 1e-9, na.rm=TRUE),
  #                                    n_sol   = sum(!is.na(get("solar_bc")) & abs(get("solar_bc") - solar) > 1e-9, na.rm=TRUE),
  #                                    n_tcdc  = sum(!is.na(get("tcdc_bc"))  & abs(get("tcdc_bc")  - tcdc)  > 1e-9, na.rm=TRUE)
  #                                  )]
  # 
  # print(changed_summary)
  # 
  
  return(row_latest_bc)
  
}

row_latest_corrected  <- try(apply_biases2(row_latest))
if (inherits(row_latest_corrected, "try-error")){
  row_latest_corrected = row_latest
}
# 
# row_latest[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td)]
# row_latest_corrected[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td)]
# # row_latest_corrected2[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td)]
# 
# 
# 




# #
# row_latest[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td)]
# 
# 
# row_latest_corrected[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td)]

############################################################--------------------------------
############################################################--------------------------------

############################################################--------------------------------
############################################################--------------------------------



############################################################--------------------------------
############################################################--------------------------------

############################################################--------------------------------
############################################################--------------------------------

# thelatestrow = row_latest_corrected

getTheLatestWBGT = function(thelatestrow){
  
  # unloadNamespace('RMySQL')
  # library(RMySQL)
  ############################################################
  ############################################################
  ############################################################
  ############################################################
  ############################################################
  # UV INDEX FORECAST DATA
  
  
  all_uv_dat = unique(thelatestrow[,c("siteRecID","validTime","uvi","uvi_clear")])
  
  all_uv_dat[, validTime := round_date(validTime, unit = "hour")]
  
  
  # thelatestrow[siteRecID == '94cb6ead-2cfc-40fb-94d0-db9f50075394']
  
  row_latest_hrrsubh = thelatestrow[model == 'hrrr_subh'] # & is.na(uvi)].
  # row_latest_hrrsubh$uv_time
  row_latest_hrrsubh[, uv_time := round_date(validTime, unit = "hour")]
  row_latest_hrrsubh$uvi = NULL
  row_latest_hrrsubh$uvi_clear = NULL
  
  # 2) ensure your UV index table is keyed by its validTime, lat_uv, lon_uv
  setkey(all_uv_dat, siteRecID, validTime)
  
  # unique(row_latest_hrrsubh, by = c("siteRecID","validTime"))
  
  row_latest_hrrsubh = merge(row_latest_hrrsubh, all_uv_dat, by.x = c('uv_time', "siteRecID"),
                             by.y = c("validTime","siteRecID"), all.x=TRUE, allow.cartesian=TRUE) # [,.N]
  
  
  
  thelatestrow = rbindlist(list(row_latest_hrrsubh, thelatestrow[model != 'hrrr_subh']), fill=TRUE)
  
  ############################################################
  ############################################################
  ############################################################
  
  # countna(thelatestrow)
  
  
  results_by_model = ''
  dt_mod = ''
  rm(results_by_model)
  rm(dt_mod)
  gc()
  
  thelatestrow = thelatestrow[order(validTime)]
  # compute the time‐difference (in minutes) from now
  thelatestrow[, timediff := difftime(validTime, Sys.time(), units = "mins")]
  
  thelatestrow[, pres := 1013.5]
  # thelatestrow[siteRecID == '3664b9bf-c3dd-4e13-835b-73e09d531274']
  ##_____________________________________________________________________
  ##_____________________________________________________________________
  ##_____________________________________________________________________
  
  # QUERY Sql table where i regularly update the values (temp, humidity, and also WBGT estimates themnselves) for ASOS/AWOS across US.
  # not currently used in the code, but lays groundwork for incorporation.
  
  
  #
  #
  # apt = getSQL2(sqlquery = 'select * from station.AirportWBGT', databaseName = 'station',
  #              serverip = '44.197.87.201', as.datetime = FALSE)
  # air_dt <- apt
  # rename.cols(apt, c("lat","lon"), c("lat_air","lon_air"))
  #
  # ## --- 2.  Ensure the site coordinates are a data.table --------------------
  # sites_with_airports = info_dt[,c("name","siteRecID","lat","lon")]
  # # site_dt <- copy(sites_coords)           # already data.table-ish
  # setnames(sites_with_airports, c("lat","lon"), c("lat_site","lon_site"))
  #
  # ## --- 3.  Compute the distance matrix (site × airport) --------------------
  # # geodist() returns metres by default
  # dist_mat_m <- geodist(
  #   x = as.matrix(sites_with_airports[, .(lon_site, lat_site)]),
  #   y = as.matrix(air_dt[, .(lon_air,  lat_air )]),
  #   measure = "haversine"
  # )
  #
  # ## --- 4.  For each site, grab the nearest airport index & distance --------
  # idx_min   <- max.col(-dist_mat_m, ties.method = "first")   # position of min dist
  # dist_min  <- dist_mat_m[cbind(seq_len(nrow(sites_with_airports)), idx_min)]
  #
  # # air_dt$wbgt[idx_min]
  # # air_dt$wind2m[idx_min]
  # # air_dt$solar[idx_min]
  # # air_dt$ta[idx_min]
  # # air_dt$rh[idx_min]
  # ## --- 5.  Bind the results back to the site table -------------------------
  # sites_with_airports[, `:=`( nearest_airport = air_dt$station[idx_min],
  #                             wbgt = air_dt$wbgt[idx_min],
  #                             wind2m = air_dt$wind2m[idx_min],
  #                             solar = air_dt$solar[idx_min],
  #                             ta = air_dt$ta[idx_min],
  #                             rh = air_dt$rh[idx_min],
  #                             dist_miles      = dist_min / 1609.344 )]   # metres → miles
  #
  # # ---- Final result --------------------------------------------------------
  # # sites_with_airports[]
  #
  # sites_with_airports = sites_with_airports[siteRecID %in% xanoSites$siteRecID]
  #
  # sites_with_airports = sites_with_airports[dist_miles < 20]
  #
  # ##_____________________________________________________________________
  ##_____________________________________________________________________
  ##_____________________________________________________________________
  
  
  
  
  rename.cols(thelatestrow, c("lcdc","mcdc","hcdc","tcdc"), c("lcdc1","mcdc1","hcdc1","tcdc1"))
  
  
  thelatestrow = thelatestrow[!is.na(lon) | !is.na(lat)]
  
  if (is.null(thelatestrow$rh)){
    rename.cols(thelatestrow, "relh", "rh")
  }
  thelatestrow <- na.omit(thelatestrow, cols = c("ta"))
  
  
  ############################################
  ############################################
  #
  # threshold_mi = 10
  # time_cutoff_min = 20
  # buf_km = 20
  
  matchGOES <- function(row_dt, threshold_mi = 10,  time_cutoff_min = 20, buf_km = 20) {
    buf_m <- buf_km * 1000  # convert to metres
    
    # 1) Read the two Parquet tables
    
    #── A) Read in your two Parquet ta
    # les ────────────────────────────────────
    #── A) Read & prepare the low‐res GOES table ────────────────────────────────
    low  <- as.data.table( collect(open_dataset('/data/sat/goes2/goes-current_v211.parquet')))
    low[ , `:=`(
      tcdc        = round(tcdc), # * 100),
      lcdc        = rowMeans(.SD[, .(lcdc1, lcdc2)], na.rm=TRUE),
      hcdc        = rowMeans(.SD[, .(hcdc1, hcdc2)], na.rm=TRUE),
      goes_clouds = 1L
    ), .SDcols = c("lcdc1","lcdc2","hcdc1","hcdc2")]
    
    # quantile(low$tcdc, na.rm=TRUE)
    
    #── B) Read & prepare the high‐res GOES‐COD table ───────────────────────────
    high <- as.data.table( collect(open_dataset("/data/sat/goes2/goes-current-cod_cmi_v211.parquet")))
    # r <- rast(
    #   x    = data.frame(x = high$lon, y = high$lat, v = high$COD),
    #   type = "xyz",
    #   crs  = "EPSG:4326"
    # )
    # plot(r)
    # countna(high)
    
    
    
    # high[!is.na(COD)]
    # range(low$tcdc, na.rm=TRUE)
    
    goesvalidTime = low$goesvalidTime[1]
    
    # 2) Turn your sites into a SpatVector
    pts <- vect(row_dt[, .(lon, lat)], crs = "EPSG:4326")
    
    # 3) Helper: for a given field name, build & extract
    # as.data.table(pts)
    
    # # avail_low extract_field, dt = low, pts = pts, buf = buf_m)
    # fld = 'COD'
    # buf = 20 * 1000 # 20 km buffer in metres
    # dt = high
    
    #── 3) A small helper: build a one‐band raster & extract center+buffer ────
    extract_field <- function(dt, fld, pts, buf) {
      # build the raster
      r <- rast(
        x    = data.frame(x = dt$lon, y = dt$lat, v = dt[[fld]]),
        type = "xyz",
        crs  = "EPSG:4326"
      )
      # plot(r)
      # extract the nearest‐pixel value
      ex_c <- extract(r, pts, method = "near")
      # extract the buffer‐mean
      ex_b <- extract(r, pts, buffer = buf, fun = mean, na.rm = TRUE)
      
      # return a two‐column data.table with safe names
      df <- data.table(match = ex_c$v, buf = ex_b$v)
      setnames(df,
               old = c("match","buf"),
               new = c(paste0(fld, "_match"), paste0(fld, "_buf")))
      df
    }
    
    extract_field2 <- function(dt, fld, pts, buf) {
      
      # if it’s a character column, we’ll rasterize the *mode* (most frequent) in the buffer
      if (is.character(dt[[fld]])) {
        # 1) turn to a factor so we can map back later
        fac   <- factor(dt[[fld]])
        levels <- levels(fac)
        codes  <- as.integer(fac)
        
        # 2) build a temporary integer raster of factor codes
        r <- rast(
          x    = data.frame(x = dt$lon, y = dt$lat, v = codes),
          type = "xyz",
          crs  = "EPSG:4326"
        )
        
        # helper to compute mode
        mode_fun <- function(x, ...) {
          x <- x[!is.na(x)]
          if (length(x)==0) return(NA_integer_)
          ux <- unique(x)
          ux[which.max(tabulate(match(x, ux)))]
        }
        
        # 3) extract nearest‐pixel code
        ex_c <- extract(r, pts, method = "near")$v
        
        # 4) extract buffer mode code
        ex_b <- extract(r, pts, buffer = buf, fun = mode_fun)$v
        
        # 5) map back to the original character levels
        df <- data.table(
          match = levels[ex_c],
          buf   = levels[ex_b]
        )
        
      } else {
        # numeric branch (unchanged from before)
        r <- rast(
          x    = data.frame(x = dt$lon, y = dt$lat, v = dt[[fld]]),
          type = "xyz",
          crs  = "EPSG:4326"
        )
        ex_c <- extract(r, pts, method = "near")$v
        ex_b <- extract(r, pts, buffer = buf, fun = mean, na.rm = TRUE)$v
        df <- data.table(match = ex_c, buf = ex_b)
      }
      
      # 6) give safe column names and return
      setnames(
        df,
        old = c("match","buf"),
        new = c(paste0(fld, "_match"), paste0(fld, "_buf"))
      )
      return(df)
    }
    
    #── 4) Low‐res fields (CCLC) ───────────────────────────────────────────────
    desired_low <- c("tcdc","lcdc","mcdc","hcdc") #,"cloud_thickness","cloud_type")
    avail_low   <- intersect(desired_low, names(low))
    missing_low <- setdiff(desired_low, names(low))
    if (length(missing_low)) {
      warning("These low‐res fields were not found and will be skipped: ",
              paste(missing_low, collapse = ", "))
    }
    low_list <- lapply(avail_low, extract_field, dt = low, pts = pts, buf = buf_m)
    low_out  <- if (length(low_list)) do.call(cbind, low_list) else NULL
    
    #── 5) High‐res fields (COD, CMI13/14, cloudTopTemp) ──────────────────────
    desired_high <- c("COD","cmi13","cmi14","cloudTopTemp")# ,'cloud_type','cloud_thickness')
    avail_high   <- intersect(desired_high, names(high))
    missing_high <- setdiff(desired_high, names(high))
    if (length(missing_high)) {
      warning("These high‐res fields were not found and will be skipped: ",
              paste(missing_high, collapse = ", "))
    }
    high_list <- lapply(avail_high, extract_field, dt = high, pts = pts, buf = buf_m)
    high_out  <- if (length(high_list)) do.call(cbind, high_list) else NULL
    
    #── 6) Bind back onto your original points ────────────────────────────────
    
    row_dt$goesvalidTime <- goesvalidTime
    result <- cbind(row_dt, low_out, high_out)
    return(result)
  }
  
  # 1) Attempt the call
  row_latest_goes <- try(matchGOES(
    row_dt = thelatestrow,
    threshold_mi = 10,
    time_cutoff_min = 20,
    buf_km       = 20
  ))
  
  # 2) If it errored, report and handle
  if (inherits(row_latest_goes, "try-error")) {
    percentagemissing = round((row_latest_goes[is.na(COD_match)][,.N] / row_latest_goes[,.N])*100)
    send_sentry_event(
      message = "matchGOES failed",
      level   = "error",
      # tags    = list(fn = "matchGOES"),
      
      tags = list(
        script    = "klimoCurrentWBGT",
        fn        = "matchGOES",
        percentmissing = percentagemissing,
        environment = "production"
      ),
      
      verbose=FALSE
    )
  }
  
  
  ##########################################
  ##########################################
  # 2) Filter to daytime only
  # row_latest_goes$validTime = now(tz='UTC')
  row_latest_goes[, cloud_thickness := ifelse(COD_match < 5, "Thin",
                                              ifelse(COD_match >= 5 & COD_match < 20, "Moderate", "Thick"))]
  # ────────────────────────────────────────────────────────────────────────────
  row_latest_goes[ , cloud_type := fifelse(COD_match > 20 & cloudTopTemp_match < 240, "Deep Convective",
                                           fifelse(COD_match <  5 & cloudTopTemp_match > 273.15, "Thin Water",
                                                   "Other")
  ) ]
  
  row_latest_goes$goes_age = as.numeric(difftime(row_latest_goes$goesvalidTime[1],
                                                 row_latest_goes$validTime, units = 'mins'))
  
  row_latest_goes[abs(goes_age) < 20 & !is.na(lcdc_match), lcdc := lcdc_match]
  row_latest_goes[abs(goes_age) < 20 & !is.na(mcdc_match), mcdc := mcdc_match]
  row_latest_goes[abs(goes_age) < 20 & !is.na(hcdc_match), hcdc := hcdc_match]
  row_latest_goes[abs(goes_age) < 20 & !is.na(tcdc_match), tcdc := tcdc_match]
  
  
  row_latest_goes[, zenith := jj::zenithinfo(jj::solarinfo(validTime), lon, lat)]
  row_latest_goes[zenith > 85, `:=`( tcdc = tcdc1, lcdc = lcdc1, mcdc = mcdc1, hcdc = hcdc1 )]
  
  
  cols2drop = names(row_latest_goes)[names(row_latest_goes) %likeany% c("buf","match")]
  
  
  row_latest_goes = row_latest_goes[, !cols2drop, with=FALSE]
  gc()
  
  # row_latest_goes$
  #####################
  coeffs       <- list(low=0.30, mid=0.60, high=0.90)
  
  row_latest_goes[tolower(cloud_thickness)  == 'thin', thick_mul := 0.5]
  row_latest_goes[tolower(cloud_thickness)  == 'moderate', thick_mul := 1.0]
  row_latest_goes[tolower(cloud_thickness)  == 'thick', thick_mul := 1.5]
  row_latest_goes[tolower(cloud_thickness) == 'dense', thick_mul := 2.0]
  
  # 3) compute fractions & thickness factor
  row_latest_goes[, `:=`(
    frac_low   = lcdc/100,
    frac_mid   = mcdc/100,
    frac_high  = hcdc/100
  )]
  
  # row_latest_goes$cloud_thickness
  # 2) compute raw remainders for *all* rows (vectors)
  row_latest_goes[, `:=`(
    rem_low_raw  = 1 - coeffs$low  * frac_low  * thick_mul,
    rem_mid_raw  = 1 - coeffs$mid  * frac_mid  * thick_mul,
    rem_high_raw = 1 - coeffs$high * frac_high * thick_mul
  )]
  
  
  # nm
  # 3) clip those to ≥0
  for (nm in c("rem_low_raw","rem_mid_raw","rem_high_raw")) {
    row_latest_goes[, (nm) := pmax(0, get(nm))]
  }
  
  # 4) flag who has *any* layer information
  row_latest_goes[, has_layers := !is.na(lcdc) | !is.na(mcdc) | !is.na(hcdc)]
  
  # row_latest_goes[is.na(rem_low_raw)]
  # 5) assign cloud_mod for layer‐cases
  row_latest_goes[has_layers == TRUE,
                  cloud_mod := rem_low_raw * rem_mid_raw * rem_high_raw
  ]
  
  # 6) fallback for pure-tcdc cases (no layers but have tcdc)
  row_latest_goes[has_layers == FALSE & !is.na(tcdc),
                  cloud_mod := pmax(0, 1 - coeffs$mid * tcdc * thick_mul)
  ]
  
  # 7) drop the helper columns if you like
  row_latest_goes[, c("rem_low_raw","rem_mid_raw","rem_high_raw","has_layers") := NULL]
  
  # impose a minimum transmission of 5 %
  min_trans <- 0.3
  row_latest_goes[, cloud_mod := pmax(cloud_mod, min_trans)]
  
  # 8) finally compute your UVI estimate
  row_latest_goes[, uvi_est := uvi_clear * cloud_mod]
  
  
  row_latest_goes[is.na(uvi_est), uvi_est := uvi]
  
  
  
  
  ########################################################################
  ########################################################################
  
  
  ########################################################################
  ########################################################################
  
  
  ########################################################################
  ########################################################################
  
  
  site_mrms = as.data.table(dbGetQuery(conn, "SELECT * FROM klimoWBGT.mrms_site_latest"))
  rename.cols(site_mrms, "id", 'siteRecID')
  
  
  row_latest_goes <- merge(row_latest_goes, site_mrms[,!c("lat","lon"), with=FALSE], by = c("siteRecID"), all.x=TRUE)
  
  
  row_latest_goes[preciprate_mm_per_hr > 1, CRAIN := 1]
  row_latest_goes[preciprate_mm_per_hr > 1, REFC := refl_lowest_dbz]
  
  
  
  # row_latest_goes[preciprate_mm_per_hr > 1]$tcdc
  # row_latest_goes[preciprate_mm_per_hr > 1]$lcdc
  # row_latest_goes[preciprate_mm_per_hr > 1]$mcdc
  # row_latest_goes[preciprate_mm_per_hr > 1]$hcdc
  row_latest_goes[preciprate_mm_per_hr > 1, tcdc := 99]
  
  row_latest_goes[preciprate_mm_per_hr > 1, lcdc := 90]
  row_latest_goes[preciprate_mm_per_hr > 1, mcdc := 85]
  row_latest_goes[preciprate_mm_per_hr > 1, hcdc := 85]
  
  #
  # row_latest_goes[siteRecID == 'Sycaten']$solar
  #
  #
  # row_latest_goes[siteRecID == 'Sycaten']$wind10m
  #
  
  ########################################################################
  ########################################################################
  
  
  ########################################################################
  ########################################################################
  
  
  ########################################################################
  ########################################################################
  
  # recalculate solar radiation based on latest GOES cloud data at multiple heights.
  
  
  # 1) direct‐sunlight estimate
  row_latest_goes[, solar := calcSolar(
    lat        = lat,
    lon        = lon,
    datetime   = validTime,
    cloudcover = tcdc/100,
    method     = "bras",
    hcdc       = hcdc/100,
    mcdc       = mcdc/100,
    lcdc       = lcdc/100
  )]
  # fallback where needed
  row_latest_goes[is.na(solar), solar := calcSolar(
    lat        = lat[is.na(solar)],
    lon        = lon[is.na(solar)],
    datetime   = validTime[is.na(solar)],
    cloudcover = tcdc[is.na(solar)]/100,
    method     = "basic"
  )]
  row_latest_goes[, solar := round(solar, 0)]
  # attr(row_latest_goes$solar, "method") <- "bras_atc_hcdc_mcdc_lcdc"
  
  # 2) “cloudy‐sky” (fixed–high‐cloud) estimate
  row_latest_goes[, solar_shade := calcSolar(
    lat        = lat,
    lon        = lon,
    datetime   = validTime,
    cloudcover = tcdc/100,
    method     = "bras",
    hcdc       = 90/100,
    mcdc       = 85/100,
    lcdc       = 85/100
  )]
  row_latest_goes[is.na(solar_shade), solar_shade := calcSolar(
    lat        = lat[is.na(solar_shade)],
    lon        = lon[is.na(solar_shade)],
    datetime   = validTime[is.na(solar_shade)],
    cloudcover = 1,   # 100%
    method     = "basic"
  )]
  # attr(row_latest_goes$solar_shade, "method") <- "bras_hcdc90_mcdc85_lcdc85"
  
  # 3) “full‐sun” (zero‐cloud) estimate
  row_latest_goes[, solar_sun := calcSolar(
    lat        = lat,
    lon        = lon,
    datetime   = validTime,
    cloudcover = tcdc/100,
    method     = "bras",
    hcdc       = 0,
    mcdc       = 0,
    lcdc       = 0
  )]
  row_latest_goes[is.na(solar_sun), solar_sun := calcSolar(
    lat        = lat[is.na(solar_sun)],
    lon        = lon[is.na(solar_sun)],
    datetime   = validTime[is.na(solar_sun)],
    cloudcover = 0,
    method     = "basic"
  )]
  # attr(row_latest_goes$solar_sun, "method") <- "bras_hcdc0_mcdc0_lcdc0"
  
  
  ########################################################################
  ########################################################################
  # Get surface roughness info for downscaling wind speeds from 10m to 2m AGL.
  source("/opt/klimo/code/get_roughness_raster.R")
  
  roughness_raster = try(get_roughness_raster(hrrr_source = 's3'))
  
  if (!inherits(roughness_raster , "try-error")) {
    # 1) Prepare your roughness SpatRaster in a projected CRS (so distances are meters)
    # r0_ll  <- roughness_raster         # your existing lon/lat SpatRaster
    r0_proj <- project(roughness_raster, "EPSG:5070")  # USA Albers (units = m)
    
    # 2) Define buffer radius in meters and build a circular & directional kernels
    radius_m <- 5e3  # e.g. 5 km
    
    # build a full circular kernel first
    circ_kern <- focalMat(r0_proj, radius_m, type="circle")
    
    # helper to zero out half of that kernel
    make_sector_kernel <- function(mat, quadrant = c("N","E","S","W")) {
      nr <- nrow(mat); nc <- ncol(mat)
      ctr_row <- (nr+1)/2; ctr_col <- (nc+1)/2
      coords  <- as.data.frame(which(mat>0, arr.ind=TRUE))
      dx <- coords$col - ctr_col
      dy <- coords$row - ctr_row
      
      keep <- switch(
        quadrant,
        N = dy >  0,
        E = dx >  0,
        S = dy <  0,
        W = dx <  0
      )
      m2 <- mat
      m2[!keep] <- 0
      m2 / sum(m2)   # normalize so sum == 1
    }
    
    dirs4   <- c("N","E","S","W")
    kernels <- setNames(lapply(dirs4, make_sector_kernel, mat=circ_kern), dirs4)
    
    # 3) Convolve once per direction (this is the heavy bit, but only 4×)
    z_dirs <- lapply(kernels, function(k) {
      focal(r0_proj, w = k, fun = sum, na.rm = TRUE, filename="", overwrite=TRUE)
    })
    z_stack_proj <- rast(z_dirs)
    names(z_stack_proj) <- paste0("z0m_", dirs4)
    
    # 4) Reproject back to lon/lat so you can extract by lon/lat points
    z_stack_ll <- project(z_stack_proj, crs(roughness_raster), method="bilinear")
    
    z_stack_proj = ''
    roughness_raster = ''
    
    # ############  ############
    # ############  ############
    dirs4   <- c("N","E","S","W")
    angles4 <- setNames(c(0,90,180,270), dirs4)
    half4   <- 45
    radius_km = 5
    
    res  <- 0.4
    r_m       <- radius_km * 1000
    dirs  =    dirs4
    angles  =  angles4
    half_ang  <- 45
    
    # ############  ############  # ############  ############  # ############  ############
    # ############  ############  # ############  ############  # ############  ############
    # ############  ############  # ############  ############  # ############  ############
    
    grid_dt = row_latest_goes[,c("lat","lon")]
    
    
    # 1) Turn your grid_dt into a SpatVector of points
    grid_pts <- vect(grid_dt, geom = c("lon","lat"), crs = "EPSG:4326")
    
    # 2) Extract all four layers at once
    #    This returns a data.frame with columns ID, z0m_N, z0m_E, z0m_S, z0m_W
    dir_vals <- terra::extract(z_stack_ll, grid_pts)
    
    
    z_stack_proj = ''
    gc()
    
    
    # 3) Bind them back onto grid_dt
    setDT(dir_vals)[, ID := NULL]      # drop the internal “ID” column
    grid_dt[, c("z0m_N","z0m_E","z0m_S","z0m_W") := dir_vals]
    
    
    
    # 1) Update‐join grid_dt’s z0m_* columns onto alldat by (lat,lon)
    setkey(grid_dt, lat, lon)
    # This is much faster than merge(..., allow.cartesian=TRUE)
    row_latest_goes[
      grid_dt,
      `:=`(
        z0m_N = i.z0m_N,
        z0m_E = i.z0m_E,
        z0m_S = i.z0m_S,
        z0m_W = i.z0m_W
      ),
      on = .(lat, lon)
    ]
    
    # 2) In one pass, pick the right z0m_* based on WDIR using fcase()
    row_latest_goes[
      ,
      sfcr := fcase(
        (WDIR <  45 | WDIR >= 315), z0m_N,  # North quadrant
        (WDIR >= 45 & WDIR < 135), z0m_E,  # East quadrant
        (WDIR >=135 & WDIR < 225), z0m_S,  # South quadrant
        (WDIR >=225 & WDIR < 315), z0m_W,  # West quadrant
        default = NA_real_
      )
    ]
    
    ##########
    # alldat = merge(alldat, hrrr_sfcr[,c("siteRecID","sfcr")], by = 'siteRecID', all.x = TRUE)
    row_latest_goes = row_latest_goes[!is.na(wind10m)]
    
    ########
    
    row_latest_goes$wind2m_sfcr = logwind(solar=row_latest_goes$solar, wind=row_latest_goes$wind10m,
                                          sfcr = TRUE, z0m = row_latest_goes$sfcr, height1low =2, height2high =10, urbanORrural = 'urban')
    row_latest_goes[sfcr == 0, wind2m_sfcr := NA]
    
    
    
  } else {
    row_latest_goes$wind2m_sfcr = NA
  }
  
  
  
  
  
  
  row_latest_goes[is.na(wind2m_sfcr), wind2m_sfcr := logwind(solar=solar, wind=wind10m, urbanORrural="urban",
                                                             vert_temp_gradient=-0.1,  height1low =2, height2high =10)]
  
  
  row_latest_goes$wind2m = NULL
  rename.cols(row_latest_goes, "wind2m_sfcr", "wind2m")
  
  row_latest_goes$wind2m = logwind(solar=row_latest_goes$solar, wind=row_latest_goes$wind10m,
                                   urbanORrural="urban",
                                   vert_temp_gradient=-0.1,  height1low =2, height2high =10)
  row_latest_goes$wind2m_shade = logwind(solar=row_latest_goes$solar_shade, wind=row_latest_goes$wind10m,
                                         urbanORrural="urban",
                                         vert_temp_gradient=-0.1,  height1low =2, height2high =10)
  
  row_latest_goes$wind2m_sun = logwind(solar=row_latest_goes$solar_sun, wind=row_latest_goes$wind10m,
                                       urbanORrural="urban",
                                       vert_temp_gradient=-0.1,  height1low =2, height2high =10)
  
  
  
  
  # windSpeedThreshold = 0.75
  # windSpeedThreshold = 1.25 # temporary setting this to 1.25 20250710 133400
  windSpeedThreshold = 0.8 # temporary setting this to 0.8 20250726141600
  
  ###########################
  # added this extra correction at 20240820175000
  windspeedextracorrection = 0.7
  row_latest_goes[wind2m > 3, wind2m := wind2m * windspeedextracorrection] # was >4 until
  row_latest_goes[wind2m > 3, wind2m_shade := wind2m_shade * windspeedextracorrection]
  row_latest_goes[wind2m > 3, wind2m_sun := wind2m_sun * windspeedextracorrection]
  
  ###########################
  windspeedextracorrection_allspeeds = 0.75 # 20250726 141900
  
  row_latest_goes[wind2m > 1.25, wind2m := wind2m * windspeedextracorrection_allspeeds]
  row_latest_goes[wind2m > 1.25, wind2m_shade := wind2m_shade * windspeedextracorrection_allspeeds]
  row_latest_goes[wind2m > 1.25, wind2m_sun := wind2m_sun * windspeedextracorrection_allspeeds]
  
  
  ###########################
  # row_latest_goes[wind2m_sfcr < windSpeedThreshold, wind2m_sfcr := windSpeedThreshold]
  row_latest_goes[wind2m < windSpeedThreshold, wind2m := windSpeedThreshold]
  row_latest_goes[wind2m_shade < windSpeedThreshold, wind2m_shade := windSpeedThreshold]
  row_latest_goes[wind2m_sun < windSpeedThreshold, wind2m_sun := windSpeedThreshold]
  
  #########################################
  #########################################
  
  # Calculate WBGT. Liljegren et al 2008 method.
  
  row_latest_goes$rh = row_latest_goes$relh
  
  
  row_latest_goes[, wbgt := calcWBGT(validTime, lat, lon, solar, pres, as.numeric(ta), rh, wind2m,
                                     zspeed = rep(2, .N), dT = rep(1, .N), urban = rep(1, .N), calcTpsy = FALSE,
                                     convergencethreshold = 0.02,
                                     inputUnits = list(ta = "degC", rh = "%", pres = "hPa", wind = "m/s", solar = "W/m^2"),
                                     outputUnits = "degC", surface_type = NULL,
                                     surface_properties = NULL, numOfThreads = 3)$wbgt]
  
  row_latest_goes[, wbgt_shade := calcWBGT(validTime, lat, lon, solar_shade, pres, as.numeric(ta), rh, wind2m_shade,
                                           zspeed = rep(2, .N), dT = rep(1, .N), urban = rep(1, .N), calcTpsy = FALSE,
                                           convergencethreshold = 0.02,
                                           inputUnits = list(ta = "degC", rh = "%", pres = "hPa", wind = "m/s", solar = "W/m^2"),
                                           outputUnits = "degC", surface_type = NULL,
                                           surface_properties = NULL, numOfThreads = 3)$wbgt]
  
  
  row_latest_goes[, wbgt_sun := calcWBGT(validTime, lat, lon, solar_sun, pres, as.numeric(ta), rh, wind2m_sun,
                                         zspeed = rep(2, .N), dT = rep(1, .N), urban = rep(1, .N), calcTpsy = FALSE,
                                         convergencethreshold = 0.02,
                                         inputUnits = list(ta = "degC", rh = "%", pres = "hPa", wind = "m/s", solar = "W/m^2"),
                                         outputUnits = "degC", surface_type = NULL,
                                         surface_properties = NULL, numOfThreads = 3)$wbgt]
  
  
  unit(row_latest_goes$wbgt) <- 'degF'
  unit(row_latest_goes$wbgt_shade) <- 'degF'
  unit(row_latest_goes$wbgt_sun) <- 'degF'
  unit(row_latest_goes$ta) <- 'degF'
  
  unit(row_latest_goes$wind2m) <- 'mph'
  unit(row_latest_goes$wind10m) <- 'mph'
  
  
  row_latest_goes = row_latest_goes[!is.na(ta)]
  
  # 
  # 
  # row_latest_goes[preciprate_mm_per_hr > 1]$wbgt
  # row_latest_goes[preciprate_mm_per_hr > 1]$solar
  # 
  # 
  # # 
  # # >   row_latest_goes[preciprate_mm_per_hr > 1]$wbgt
  # # [1] 66.95039 62.89650 65.54816
  # attr(,"unit")
  # [1] "degF"
  # >   row_latest_goes[preciprate_mm_per_hr > 1]$solar
  # [1] 116  83 119
  # > 
  # #########################################
  # # #########################################
  # # # Process WBGT and other temperature columns
  wbgt_cols <- c("wbgt", "wbgt_shade", "wbgt_sun",'ta')
  
  row_latest_goes[, (wbgt_cols) := lapply(.SD, round, digits = 1), .SDcols = wbgt_cols]
  
  
  
  # # #########################################
  # # #########################################
  row_latest_goes$hi = round(calcHI(airTemp  = row_latest_goes$ta, relativeHumidity  = row_latest_goes$rh,
                                    inputunits = "degF", outputunits = "degF"), 1)
  
  row_latest_goes = row_latest_goes[order(validTime)]
  row_latest_goes$date = j.date(row_latest_goes$validTime)
  
  return(row_latest_goes)
  
  
  
}

row_latest$rh = row_latest$relh
row_latest_corrected$rh = row_latest_corrected$relh

row_latest_uncorrected = getTheLatestWBGT(row_latest)
row_latest_corrected = try(getTheLatestWBGT(row_latest_corrected))
if (inherits(row_latest_corrected, "try-error")){
  row_latest_corrected = row_latest_uncorrected
}

# 
# #
# #
# # row_latest_uncorrected[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td, wbgt)]
# # row_latest_corrected[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td, wbgt)]
# #
# #
# #
# 
# 



keys <- c("siteRecID","validTime")
vars_wanted <- c(
  "wbgt","wbgt_shade","wbgt_sun",
  "ta","td","rh","relh",
  "wind10m","wind2m","wind2m_shade","wind2m_sun",
  "solar","solar_shade","solar_sun",
  "tcdc","lcdc","mcdc","hcdc",
  "uvi","uvi_est",
  'CRAIN','REFC'
)
vars_keep <- intersect(vars_wanted, intersect(names(row_latest_uncorrected), names(row_latest_corrected)))
merged_uc <- merge(
  row_latest_uncorrected[, c(keys, vars_keep), with = FALSE],
  row_latest_corrected[,  c(keys, vars_keep), with = FALSE],
  by = keys, all = TRUE,
  suffixes = c("_uncorr","_corr")  # <- your distinct suffixes
)

# columns where % change makes sense
mult_vars <- intersect(c("wind10m","wind2m","solar","solar_shade","solar_sun"), vars_keep)

for (v in vars_keep) {
  merged_uc[, (paste0(v, "_delta")) := get(paste0(v,"_corr")) - get(paste0(v,"_uncorr"))]
  if (v %in% mult_vars) {
    merged_uc[, (paste0(v, "_pct")) :=
                fifelse(is.finite(get(paste0(v,"_uncorr"))) & get(paste0(v,"_uncorr")) != 0,
                        100 * (get(paste0(v,"_corr")) / get(paste0(v,"_uncorr")) - 1),
                        NA_real_)]
  }
}




# 
# # quick peek for one site
# merged_uc[siteRecID == "Sycaten",
#           .(siteRecID, validTime, ta_uncorr, ta_corr, ta_delta, wbgt_delta,
#             wind10m_uncorr, wind10m_corr, wind10m_pct)][order(validTime)]
# # 
# # merged_uc$wbgt_corr
# # 
# # merged_uc[siteRecID == "Sycaten"][,c('wbgt_corr','wbgt_uncorr')]
# # 
# # 
# 
# 
# 
# # merged_uc[,c("wbgt_uncorr","wbgt_corr")]
# 



# row_latest_corrected$solar_sun
############################
##############################
##############################
##############################
row_latest_corrected[tcdc > 100, tcdc := 100]
row_latest_corrected[tcdc < 0, tcdc := 0]


row_latest_goes = row_latest_corrected



row_latest_uncorrected$wbgt_raw = row_latest_uncorrected$wbgt
row_latest_uncorrected$wind10m_raw = round(row_latest_uncorrected$wind10m, 1)
row_latest_uncorrected$wind2m_raw = round(row_latest_uncorrected$wind2m, 1)
row_latest_uncorrected$ta_raw = row_latest_uncorrected$ta
row_latest_uncorrected$rh_raw = row_latest_uncorrected$rh
row_latest_uncorrected$solar_raw = row_latest_uncorrected$solar
row_latest_uncorrected$td_raw = row_latest_uncorrected$td
row_latest_uncorrected$tcdc_raw = row_latest_uncorrected$tcdc

unit(row_latest_uncorrected$td_raw) <- 'degF (1)'



# colnames(row_latest_goes)[colnames(row_latest_goes) %like% "lkcdc"]




row_latest_goes <- row_latest_goes[, !names(row_latest_goes) %in% c("lcdc1","mcdc1","hcdc1","tcdc1"), with = FALSE]

row_latest_goes <- merge(
  row_latest_goes,
  row_latest_uncorrected[, c("siteRecID","wbgt_raw","wind10m_raw",
                             "wind2m_raw","ta_raw","rh_raw",
                             "solar_raw","td_raw","tcdc_raw")],
  by = "siteRecID"
)


# row_latest_goes$wbgt_raw

# 
# 
# 
# row_latest_goes = merge(row_latest_goes,
#                         row_latest_uncorrected[,c("siteRecID","wbgt_raw","wind10m_raw","wind2m_raw",
#                                                   "ta_raw","rh_raw","solar_raw","td_raw","tcdc_raw")],
#                         by = 'siteRecID')

# 
# # row_latest_goes$wbgt_raw
# 
# #
# #
# #
# # row_latest_uncorrected[ta == 111.9]
# #
# 
# 
# # row_latest_uncorrected[siteRecID == 'Sycaten', .(validTime, ta, rh, wind10m, solar, tcdc, td, wbgt)]
# # row_latest_corrected[siteRecID == 'Sycaten', .(validTime, ta, relh, wind10m, solar, tcdc, td, wbgt)]
# # #
# # #
# #
# DT1 = row_latest_uncorrected[,c("siteRecID","wbgt")]
# 
# rename.cols(DT1, "wbgt", "wbgtu")
# 
# DT2 = row_latest_corrected[,c("siteRecID","wbgt")]
# 
# #
# DTboth = merge(DT1, DT2, by = "siteRecID", all = TRUE)
# #
# DTboth$diff = DTboth$wbgt - DTboth$wbgtu
# #
# #
# boxplot(DTboth$diff)
# #
# unique(DTboth$diff)
# 
# DTboth[order(-wbgt)][1:20]
# DTboth[order(-diff)][1:20]
# 

########################################################################################################################
########################################################################################################################
########################################################################################################################



########################################################################################################################
########################################################################################################################
########################################################################################################################



########################################################################################################################
########################################################################################################################
########################################################################################################################

# unloadNamespace('RMySQL')

# Compare with previous WBGT to gauge change in previous estimate compared to latest one.

# previousWBGT = getSQL2("select * from klimoWBGT.current where validTime = (select max(validTime) from klimoWBGT.current)",
#                       databaseName = "klimoWBGT", serverip = Sys.getenv("SQL_SERVER_IP"), as.datetime = FALSE)

previousWBGT = dbGetQuery(conn, "select * from klimoWBGT.current where validTime = (select max(validTime) from klimoWBGT.current)")
setDT(previousWBGT)






# threshold can be changed if needed
flag_wbgt_changes <- function(previousWBGT, row_latest_goes, threshold = 10) {
  # ensure data.table
  previousWBGT    <- as.data.table(previousWBGT)
  row_latest_goes <- as.data.table(row_latest_goes)
  
  # keep one row per site in each table (latest by validTime)
  prev <- previousWBGT[order(siteRecID, -validTime),
                       .SD[1], by = siteRecID][,
                                               .(siteRecID, prev_wbgt = wbgt, prev_validTime = validTime)]
  
  curr <- row_latest_goes[order(siteRecID, -validTime),
                          .SD[1], by = siteRecID][,
                                                  .(siteRecID, latest_wbgt = wbgt, latest_validTime = validTime)]
  
  # inner join on siteRecID (only sites present in both)
  out <- merge(prev, curr, by = "siteRecID", all = FALSE)
  
  # compute differences and flag
  out[, diff        := latest_wbgt - prev_wbgt]
  out[, diff_abs    := abs(diff)]
  out[, flag_change := !is.na(diff_abs) & diff_abs > threshold]
  
  # optional: return flagged first
  setorder(out, -flag_change, siteRecID)
  return(out[])
}


wbgt_flagged <- flag_wbgt_changes(previousWBGT[, .(siteRecID, wbgt, validTime)],
                                  row_latest_goes[, .(siteRecID, wbgt, validTime)],
                                  threshold = 10)

# wbgt_flagged[1:20]
# > wbgt_flagged[1:20]
# siteRecID prev_wbgt
# <char>     <num>
#   1:        2e82c1c8-361e-41d6-9dbb-ffe91f7c93de        54
# 2:        42b53b88-685d-4164-986c-3a6ffdee33f2        63
# 3:        44e2c02c-b07b-4ad8-b783-356c0c9a803f        53
# 4:        876c34aa-665d-4dec-ab30-92d32902da73        74
# 5:        a7036856-8651-41d4-b673-9a8cd2c9f7a3        64
# 6:        abb2b48a-9d48-43bf-a8e2-39b5e51f73b2        53
# 7:        ec0c6f94-b50b-4c17-b951-454721b94c0c        53
# 8:        fe173db6-e990-4fec-ac13-9b24c2c7c3a9        54
# 9:    xxe6578361-b0ba-4258-a38a-b07d11c7b9f92e        53
# 10: xxe657836sss1-b0ba-4258-a38a-b07d11c7b9f92e        51
# 11:        0089eee8-cb7c-44a7-b023-c4e375f8ce0e        74
# 12:        04759905-2b58-48e5-9781-1830a318b01d        68
# 13:        04af01a9-ded3-42fc-af08-2a31bfb82f51        67
# 14:        05b46f8b-564b-4e46-bc51-fee930708d12        69
# 15:        065deeb6-0ca4-4791-8084-b385d062a5cb        69
# 16:        096bac54-4676-4866-9434-09d7aa7c62a8        69
# 17:        09aec707-6273-43f4-9917-99638904237a        69
# 18:        0a06c770-a4d9-414d-8f17-f058acfb3bc6        68
# 19:        0b1d6aa3-1f8b-4dd8-9e53-fc3f5028aca6        71
# 20:        0ca22fd1-ef26-41c9-9c01-a2b4c49f2770        69
# siteRecID prev_wbgt
# prev_validTime latest_wbgt    latest_validTime  diff diff_abs
# <POSc>       <num>              <POSc> <num>    <num>
#   1: 2025-09-01 14:23:55        66.1 2025-09-01 16:08:55  12.1     12.1
# 2: 2025-09-01 14:23:55        75.3 2025-09-01 16:08:55  12.3     12.3
# 3: 2025-09-01 14:23:55        67.0 2025-09-01 16:08:55  14.0     14.0
# 4: 2025-09-01 14:23:55        84.8 2025-09-01 16:08:55  10.8     10.8
# 5: 2025-09-01 14:23:55        74.7 2025-09-01 16:08:55  10.7     10.7
# 6: 2025-09-01 14:23:55        67.0 2025-09-01 16:08:55  14.0     14.0
# 7: 2025-09-01 14:23:55        67.0 2025-09-01 16:08:55  14.0     14.0
# 8: 2025-09-01 14:23:55        66.1 2025-09-01 16:08:55  12.1     12.1
# 9: 2025-09-01 14:23:55        67.0 2025-09-01 16:08:55  14.0     14.0
# 10: 2025-09-01 14:23:55        62.9 2025-09-01 16:08:55  11.9     11.9

if (any(wbgt_flagged$flag_change == TRUE)){
  slack_message = sprintf("ALERT: RAPID WBGT CHANGE at %s sites, avg change of %s°F",
                          nrow(wbgt_flagged[flag_change == TRUE]),
                          mean(wbgt_flagged$diff_abs, na.rm = TRUE))
  system(sprintf("Rscript /opt/klimo/code/slackMessage.R '%s' '%s'",slack_message, 'current_alerts'))
}




# wbgt_flagged already computed by your function:
# wbgt_flagged <- flag_wbgt_changes(...)

# prepare only the columns you want to add back
add_cols <- as.data.table(wbgt_flagged)[
  , .(siteRecID,
      prev_wbgt       = prev_wbgt,
      prev_validTime  = prev_validTime,
      wbgt_delta      = diff,
      wbgt_delta_abs  = diff_abs,
      wbgt_rapid_flag = flag_change)
]

# ensure row_latest_goes is a data.table
setDT(row_latest_goes)

# update join (left join onto row_latest_goes by siteRecID)
row_latest_goes[
  add_cols,
  `:=`(
    prev_wbgt       = i.prev_wbgt,
    prev_validTime  = i.prev_validTime,
    wbgt_delta      = i.wbgt_delta,
    wbgt_delta_abs  = i.wbgt_delta_abs,
    wbgt_rapid_flag = i.wbgt_rapid_flag
  ),
  on = "siteRecID"
]


datetimestamp = substr(jj::now(char=TRUE), 1, 15)

parquetfilename = sprintf("/data/archive/klimoCurrentWBGT/%s.parquet", datetimestamp)
arrow::write_parquet(row_latest_goes, parquetfilename)

parquetfilename_with_biascorrection_and_original = sprintf("/data/archive/klimoCurrentWBGT/%s-biascorrection.parquet", datetimestamp)
arrow::write_parquet(merged_uc, parquetfilename_with_biascorrection_and_original)

# jj::timed("end")

########################################################################################################################
########################################################################################################################
########################################################################################################################

row_latest_goes = merge(row_latest_goes, info_dt[,c("siteRecID","name")], by = 'siteRecID')
# row_latest_goes$CRAIN

########################################################################################################################
########################################################################################################################
########################################################################################################################

row_latest_goes$wbgt_diff = row_latest_goes$wbgt_raw - row_latest_goes$wbgt
row_latest_goes$wbgt_diff_abs = abs(row_latest_goes$wbgt_raw - row_latest_goes$wbgt)

row_latest_goes[, wbgt_sun := ceiling(wbgt_sun)]
row_latest_goes[, wbgt_shade := floor(wbgt_shade)]
row_latest_goes[, hi := round(hi)]
row_latest_goes[, ta := round(ta)]
row_latest_goes[, cloudcover := round(as.numeric(tcdc))]
row_latest_goes[, wind2m := round(wind2m)]
row_latest_goes[, solar := round(solar)]

###########################################################################################################################
###########################################################################################################################


row_latest_goes[, rh :=  as.integer(round(rh))]

row_latest_goes[is.na(tcdc), tcdc := calcClouds(solar, lat, lon, validTime)]
row_latest_goes[, tcdc := round(tcdc)]

row_latest_goes[solar_sun == 0, daytime := 0]
row_latest_goes[solar_sun > 0, daytime := 1]

row_latest_goes[, index := .GRP, by = .(lat, lon)]

# row_latest_goes$solar

# 3) List all the columns you want to coerce
num_cols <- c("solar", "solar_shade","solar_sun","tcdc", "hi",'wbgt','wbgt_shade','wbgt_sun')
# 4) Convert them all to numeric in one line
row_latest_goes[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]



############################################################
############################################################
############################################################

row_latest_goes[!is.na(tcdc) | !is.na(lcdc) & !is.na(mcdc) & !is.na(hcdc)]


if (nrow(row_latest_goes[!is.na(tcdc) | !is.na(lcdc) & !is.na(mcdc) & !is.na(hcdc)]) != 0){
  row_latest_goes <- row_latest_goes[!is.na(tcdc) | !is.na(lcdc) & !is.na(mcdc) & !is.na(hcdc)] # [!is.na(tcdc)]
}


row_latest_goes <- row_latest_goes[, .SD[which.min(abs(timediff))], by = siteRecID]

############################################################
############################################################
############################################################

get_weather_info <- function(tcdc, wind2m, CRAIN, daytime) {
  # Convert daytime to logical
  daytime <- as.logical(daytime)
  
  # Handle cases where tcdc is NA
  tcdc <- ifelse(is.na(tcdc), 0, tcdc)
  
  # Vectorized determination of weather description
  desc <- ifelse(CRAIN == 1 & tcdc > 90, "Rain",
                 ifelse(CRAIN == 1 & tcdc > 60, "Scattered Showers",
                        ifelse(CRAIN == 1 & tcdc > 30, "Partly Cloudy with Isolated Showers",
                               ifelse(CRAIN == 1, "Light Rain",
                                      ifelse(tcdc > 80, "Cloudy",
                                             ifelse(tcdc > 60, "Mostly Cloudy",
                                                    ifelse(tcdc > 30, "Partly Cloudy",
                                                           ifelse(daytime, "Sunny", "Clear"))))))))
  
  # Vectorized determination of icons based on description and daytime
  icon <- #ifelse(CRAIN == 1 & tcdc > 90, ifelse(daytime, "adobe-heavy-rain", "adobe-heavy-rain"),
    ifelse(CRAIN == 1 & tcdc > 90, ifelse(daytime, "adobe-day-rain", "adobe-night-rain"),
           ifelse(CRAIN == 1 & tcdc > 60, ifelse(daytime, "adobe-day-scattered-showers", "adobe-night-scattered-showers"),
                  ifelse(CRAIN == 1 & tcdc > 30, ifelse(daytime, "adobe-day-scattered-showers", "adobe-night-scattered-showers"),
                         ifelse(CRAIN == 1, ifelse(daytime, "adobe-day-rain", "adobe-night-rain"),
                                ifelse(tcdc > 80, ifelse(daytime, "adobe-day-cloudy", "adobe-night-cloudy"),
                                       ifelse(tcdc > 60, ifelse(daytime, "adobe-day-mostly-cloudy", "adobe-night-mostly-cloudy"),
                                              ifelse(tcdc > 30, ifelse(daytime, "adobe-day-partly-cloudy", "adobe-night-partly-cloudy"),
                                                     ifelse(daytime, "adobe-sun", "adobe-moon"))))))))
  
  # Add wind description if applicable
  # desc <- ifelse(wind2m >= 25, paste(desc, "and Windy"), desc)
  
  return(list(desc = desc, icon = icon))
}

# Applying the function to your data
row_latest_goes[, c("weatherDesc", "icon_name") := get_weather_info(tcdc, wind2m, CRAIN, daytime)]


# row_latest_goes

row_latest_goes[, windDesc := fifelse(as.numeric(wind2m) < 2, "Calm",
                                      fifelse(as.numeric(wind2m) < 4, "Light Wind",
                                              fifelse(as.numeric(wind2m) < 6.5, "Moderate Wind",
                                                      fifelse(as.numeric(wind2m) >= 6.5 & as.numeric(wind2m) < 13, "Breezy", "Windy")
                                              )))]



.adobe_to_emoji <- c(
  "adobe-day-rain"                         = "🌧️",
  "adobe-night-rain"                       = "🌧️",
  "adobe-day-scattered-showers"            = "🌦️",
  "adobe-night-scattered-showers"          = "🌦️",
  "adobe-day-cloudy"                       = "🌥️",
  "adobe-night-cloudy"                     = "🌥️",
  "adobe-day-mostly-cloudy"                = "⛅️",
  "adobe-night-mostly-cloudy"              = "⛅️",
  "adobe-day-partly-cloudy"                = "🌤️",
  "adobe-night-partly-cloudy"              = "🌤️",
  "adobe-sun"                              = "☀️",
  "adobe-moon"                             = "🌙"
)

# 2) Create a helper to map names → emoji
get_actual_icon <- function(icon_names) {
  icons <- .adobe_to_emoji[icon_names]
  # any unknown keys become a default
  icons[is.na(icons)] <- ""
  icons
}


row_latest_goes[, icon_name := get_actual_icon(icon_name)]

##########

.weather_icon_map <- c(
  "Sunny"          = "☀️",
  "Clear"  =  emoji::emoji('full-moon'),
  "Partly Cloudy"  = "🌤️",
  "Mostly Cloudy"  = "⛅️",
  "Cloudy"         = "☁️",
  "Scattered Showers" = "🌧️",
  "Rain"           = "🌧️"
)
#
get_weather_icon <- function(desc_chr) {
  # ensure character
  desc_chr <- as.character(desc_chr)
  # lookup in the map, unknown keys become NA
  icons <- .weather_icon_map[desc_chr]
  # optionally replace NA with a default icon
  icons[is.na(icons)] <- ""
  icons
}

row_latest_goes[, weatherIcon := get_weather_icon(weatherDesc)]
row_latest_goes[, icon_name := get_weather_icon(weatherDesc)]
row_latest_goes[, conditionsIcon := get_weather_icon(weatherDesc)]

# row_latest_goes[,.N,by=weatherDesc]
# 
# row_latest_goes[CRAIN == 1]
# 
# row_latest_goes[conditionsIcon == '']
# row_latest_goes[,.N,by=conditionsIcon]
# 
# 

############################################################
############################################################
############################################################


# row_latest_goes$wbgt = ceiling(row_latest_goes$wbgt)

row_latest_goes[wbgt_sun > wbgt, wbgt_sun := wbgt]

# 1) Find rows with missing tcdc
missing_tcdc <- row_latest_goes[is.na(tcdc)]
# unique(missing_tcdc)

# 2) If any, report each one to Sentry
if (nrow(missing_tcdc) > 0) {
  for (i in seq_len(nrow(missing_tcdc))) {
    this_row <- missing_tcdc[i]
    
    sentry_error(
      message = "Missing Cloudcover for Current WBGT",
      tags = list(
        fn         = "Cloudcover",               # function name
        siteRecID  = this_row$siteRecID,
        script     = get_script_name()          # auto‐added by the helper anyway
      ),
      extra = list(
        goesvalidTime    = as.character(this_row$validTime),
        model = this_row$model
      )
    )
  }
}



############################################################
############################################################
############################################################

# currentWBGT = round(as.numeric(row_latest_goes$wbgt), 0)
#
# format_iso8601 <- function(x) {
#   # coerce to POSIXct (assume UTC if no tz)
#   t <- as.POSIXct(x, tz = "UTC")
#   # format as "YYYY-MM-DDThh:mm:ssZ"
#   strftime(t, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
# }

# row_latest_goes1 = copy(row_latest_goes)

############################################################
############################################################
############################################################
############################################################
# Policy stuff
# all_policies =  getSQL2(('select * from klimoWBGT.policies'),
#                            databaseName = "klimoWBGT", serverip = Sys.getenv('SQL_SERVER_IP'), as.datetime = FALSE)
# 
# 
all_policies = dbGetQuery(conn, "select * from klimoWBGT.policies")
setDT(all_policies)


all_policies[is.na(wbgtmin), wbgtmin := 0]
all_policies[is.na(wbgtmax), wbgtmax := 110]
all_policies = all_policies[,c("policyColor","policyDetails","policyName","wbgtmin","wbgtmax",
                               "policyPriority","policyRecID","policyGroupID")]
all_policies = unique(all_policies)


# --- Load policies once ------------------------------------------------------
load_policies <- function() {
  # pol <- getSQL2('SELECT * FROM klimoWBGT.policies',
  #               databaseName = "klimoWBGT",
  #               serverip     = conn_ip,
  #               as.datetime  = FALSE)
  # 
  pol = dbGetQuery(conn, "select * from klimoWBGT.policies")
  setDT(pol)
  
  
  setDT(pol)
  pol[, wbgtmin := as.numeric(wbgtmin)]
  pol[, wbgtmax := as.numeric(wbgtmax)]
  pol[is.na(wbgtmin), wbgtmin := 0]
  pol[is.na(wbgtmax), wbgtmax := 110]
  
  # Make bins half-open: [wbgtmin, wbgtmax)
  # (subtract a tiny epsilon from the upper edge to avoid boundary double-matches)
  eps <- 1e-8
  pol[, wbgtmax_open := wbgtmax - eps]
  
  # Keep only what we need
  pol <- unique(pol[, .(policyGroupID, policyRecID, policyName, policyDetails,
                        policyColor, policyPriority, wbgtmin, wbgtmax_open)])
  # Key for group-aware interval join
  setkey(pol, policyGroupID, wbgtmin, wbgtmax_open)
  pol[]
}

# --- Vectorized assignment ----------------------------------------------------
assign_policies_dt <- function(rows_dt,
                               policies_dt,
                               default_group = "recZo1it9hFeWOomE",
                               wbgt_col = "wbgt",
                               group_col = "policyGroupID") {
  stopifnot(is.data.table(rows_dt))
  if (!all(c(wbgt_col, group_col) %in% names(rows_dt))) {
    stop(sprintf("Need columns '%s' and '%s' in rows_dt.", wbgt_col, group_col))
  }
  
  # Ensure inputs
  rows <- copy(rows_dt)
  rows[, (wbgt_col) := as.numeric(get(wbgt_col))]
  rows[is.na(get(group_col)) | get(group_col) == "", (group_col) := default_group]
  
  # Represent each point as a degenerate interval [wbgt, wbgt]
  rows[, wbgt_lo := get(wbgt_col)]
  rows[, wbgt_hi := get(wbgt_col)]
  
  # Keys for overlap join by group + interval
  setkeyv(rows, c(group_col, "wbgt_lo", "wbgt_hi"))
  # policies_dt must be keyed on (policyGroupID, wbgtmin, wbgtmax_open)
  stopifnot(identical(key(policies_dt), c("policyGroupID","wbgtmin","wbgtmax_open")))
  
  joined <- foverlaps(
    rows, policies_dt,
    by.x = c(group_col, "wbgt_lo", "wbgt_hi"),
    by.y = c("policyGroupID","wbgtmin","wbgtmax_open"),
    nomatch = NA_integer_
  )
  
  # If a row matched multiple policies (overlapping bins), pick:
  # 1) highest policyPriority, 2) if tie, largest wbgtmin (tightest/highest bin)
  setorder(joined, siteRecID, validTime, -policyPriority, -wbgtmin)
  chosen <- joined[, .SD[1], by = .(siteRecID, validTime)]
  
  # Post-process: normalize color for Clear Flag
  chosen[policyName == "Clear Flag", policyColor := "#F8F8F8"]
  
  # Select tidy payload to merge back
  chosen_out <- chosen[, .(siteRecID, validTime,
                           policyGroupID,
                           policyName, policyRecID,
                           policyDetails, policyColor, policyPriority)]
  
  chosen_out[]
}

# --- Example usage -----------------------------------------------------------
# 1) load policies once
policies <- load_policies()

# 2) ensure your table has wbgt + policyGroupID (fill blanks with default)
row_latest_goes[is.na(policyGroupID) | policyGroupID == "", policyGroupID := "recZo1it9hFeWOomE"]

# 3) assign for the whole table in one shot
assigned <- assign_policies_dt(row_latest_goes, policies)

# 4) merge back to your rows (keeps any existing columns intact)
row_latest_goes <- merge(
  row_latest_goes,
  assigned,
  by = c("siteRecID","validTime","policyGroupID"),
  all.x = TRUE
)




row_latest_goes$windDesc


###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################




###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################
###################################################################################################


# con <- dbConnect(  RMySQL::MySQL(),   user = "jordan",   password = "wxther50!!@@",
#                    dbname = 'klimoWBGT',   host = Sys.getenv("SQL_SERVER_IP"),   port = 3306,   local_infile = TRUE)
# 
# 
# 

# unloadNamespace('RMySQL')
# 
# con <- dbConnect(
#   RMariaDB::MariaDB(),
#   user         = "jordan",
#   password     = "wxther50!!@@",
#   dbname       = "klimoWBGT",
#   host         = "44.197.87.201",
#   port         = 3306,
#   local_infile = TRUE # required for LOAD DATA LOCAL INFILE
# )



#########################################################################
# asq = getSQL2(sqlquery = 'select * from current limit 1', databaseName = 'klimoWBGT', serverip = Sys.getenv("SQL_SERVER_IP"), as.datetime = FALSE)
# asq$policyColor
asq = dbGetQuery(conn,  'select * from current limit 1')
setDT(asq)

colnames(asq)[!colnames(asq) %in% colnames(row_latest_goes)]

# stn$name
# row_latest_goes$policyDescription = row_latest_goes$policyDetails

cols2drop = colnames(row_latest_goes)[!colnames(row_latest_goes) %in% colnames(asq)]
row_latest_goes = row_latest_goes[, !cols2drop, with  = FALSE]





row_latest_goes$updated_at = jj::now(tz='UTC')



#####################################
#####################################


# i = 1
# # i = which(row_latest_goes$siteRecID == '94cb6ead-2cfc-40fb-94d0-db9f50075394')
# 
# # row_latest_goes[siteRecID == '2b1e7267-7c4e-4dd1-aa86-0fbbdaaa64b3']$rh
# # which(row_latest_goes$siteRecID == '2b1e7267-7c4e-4dd1-aa86-0fbbdaaa64b3')
# # i=17
# jj::timed('start')
# 
# # stn[name == 'Shelby']$validTime
# # Iterate through each row in the data.table
# for (i in 1:nrow(row_latest_goes)) {
#   currec = row_latest_goes[i]
#   # Construct the update query with values directly
#   columns <- names(currec)
#   values <- sapply(currec, sanitize_value)
#   column_names <- paste(columns, collapse = ", ")
#   value_list <- paste(values, collapse = ", ")
#   set_clause <- paste(columns, "=VALUES(", columns, ")", collapse = ", ")
#   query <- paste0("INSERT INTO klimoWBGT.current (", column_names, ") VALUES (", value_list, ")
#                   ON DUPLICATE KEY UPDATE ", set_clause)
#   # Execute the query
#   dbExecute(conn, query)
# }
# # jj::timed('stop')
# 
# dbDisconnect(conn)
# 
# 
# timed('end')


#
#
#
#
# timed('start')
#
#
# # ---------------------------------------------------------------------
# con <- dbConnect(  RMySQL::MySQL(),   user = "jordan",   password = "wxther50!!@@",
#                    dbname = 'klimoWBGT',   host = Sys.getenv("SQL_SERVER_IP"),   port = 3306,   local_infile = TRUE)
#
#
# suppressPackageStartupMessages({
#   library(data.table)
#   library(DBI)
#   library(RMariaDB)
# })
#
# # ---- 1) Vectorize the R-side transforms -------------------------------------
#

prepare_current_payload <- function(row_latest_goes) {
  dt <- as.data.table(row_latest_goes)
  
  # round/derive only if columns exist
  cols_round <- c("wbgt","wbgt_sun","wbgt_shade","hi","ta","wind2m","solar")
  for (cc in intersect(cols_round, names(dt))) dt[, (cc) := round(get(cc))]
  
  if ("tcdc" %in% names(dt)) {
    dt[, cloudcover := round(as.numeric(tcdc))]
  }
  
  # ---- policy mapping in bulk: compute once per unique (wbgt, policyGroupID)
  # if (!all(c("wbgt","policyGroupID") %in% names(dt))) {
  #   stop("row_latest_goes must have wbgt and policyGroupID to assign policy.")
  # }
  # 
  # keys <- unique(dt[, .(wbgt, policyGroupID)])
  # get_policy_row <- function(w, g) {
  #   p <- assign_policy_api(w, g)
  #   # pick the first row if a data.frame/data.table is returned
  #   if (is.data.frame(p)) p <- p[1, , drop = FALSE]
  #   # tweak color for "Clear Flag"
  #   if (!is.null(p$policyName) && identical(p$policyName, "Clear Flag")) {
  #     p$policyColor <- "#F8F8F8"
  #   }
  #   # return a named list with the expected fields
  #   list(
  #     policyName        = p$policyName,
  #     policyDescription = p$policyDescription,
  #     policyRecID       = p$policyRecID,
  #     policyColor       = p$policyColor,
  #     policyPriority    = p$policyPriority
  #   )
  # }
  # 
  # pol_list <- mapply(
  #   FUN = get_policy_row,
  #   w = keys$wbgt,
  #   g = keys$policyGroupID,
  #   SIMPLIFY = FALSE
  # )
  # policies <- rbindlist(pol_list, fill = TRUE)
  # keys <- cbind(keys, policies)
  # 
  # # join policies back
  # setkeyv(dt, c("wbgt","policyGroupID"))
  # setkeyv(keys, c("wbgt","policyGroupID"))
  # dt <- keys[dt]
  
  # return the payload ready to write; keep only columns that exist in DB table
  dt[]
}

# ---- 2) Bulk upsert via staging table ---------------------------------------

bulk_upsert_current <- function(payload, con, target_table = "klimoWBGT.current",
                                staging_table = "klimoWBGT__stg_current", key_cols = c("siteRecID")) {
  
  stopifnot(nrow(payload) > 0L)
  
  # Get target schema to align columns/order
  tgt_cols <- dbGetQuery(con, paste0("SHOW COLUMNS FROM ", target_table))$Field
  missing_in_payload <- setdiff(tgt_cols, names(payload))
  if (length(missing_in_payload)) {
    # add missing columns as NA so INSERT column list matches the target
    for (mc in missing_in_payload) payload[, (mc) := NA]
  }
  # order columns exactly as target table
  payload <- payload[, ..tgt_cols]
  
  # Use a transaction for speed/atomicity
  dbBegin(con)
  on.exit(try(dbRollback(con), silent = TRUE), add = TRUE)
  
  # Drop/create staging (temporary or regular)
  if (dbExistsTable(con, staging_table)) dbRemoveTable(con, staging_table)
  # Create staging with same structure as target
  dbExecute(con, sprintf("CREATE TABLE %s LIKE %s", staging_table, target_table))
  
  # Bulk write payload into staging
  dbWriteTable(con, staging_table, payload, append = TRUE, row.names = FALSE)
  
  # Build ON DUPLICATE KEY UPDATE clause (don’t overwrite primary/unique key columns)
  updatable_cols <- setdiff(tgt_cols, key_cols)
  set_clause <- paste0(
    sprintf("%s = VALUES(%s)", updatable_cols, updatable_cols),
    collapse = ", "
  )
  
  # Insert-select upsert
  sql <- sprintf(
    "INSERT INTO %1$s (%2$s)
     SELECT %2$s FROM %3$s
     ON DUPLICATE KEY UPDATE %4$s",
    target_table,
    paste(tgt_cols, collapse = ", "),
    staging_table,
    set_clause
  )
  dbExecute(con, sql)
  
  # Clean up staging
  dbRemoveTable(con, staging_table)
  
  dbCommit(con)
  invisible(TRUE)
}


bulk_upsert_current <- function(payload, con,
                                target_table  = "`klimoWBGT`.`current`",
                                staging_table = "tmp_stg_current",
                                key_cols      = c("siteRecID")) {
  
  stopifnot(nrow(payload) > 0L)
  
  # Inspect target schema
  col_meta <- DBI::dbGetQuery(con, sprintf("SHOW COLUMNS FROM %s", target_table))
  tgt_all  <- col_meta$Field
  auto_inc <- col_meta$Field[grepl("auto_increment", col_meta$Extra, ignore.case = TRUE)]
  generated <- col_meta$Field[grepl("generated", col_meta$Extra, ignore.case = TRUE)]
  skip_cols <- union(auto_inc, generated)            # e.g., c("id")
  ins_cols  <- setdiff(tgt_all, skip_cols)           # columns we will insert/update
  
  # Align payload to insertable columns only (no id)
  miss <- setdiff(ins_cols, names(payload))
  if (length(miss)) for (mc in miss) payload[, (mc) := NA]
  payload <- payload[, ..ins_cols]
  
  DBI::dbBegin(con)
  on.exit(try(DBI::dbRollback(con), silent = TRUE), add = TRUE)
  
  # Use a TEMP table so it auto-drops on disconnect
  DBI::dbExecute(con, sprintf("DROP TEMPORARY TABLE IF EXISTS %s", staging_table))
  DBI::dbExecute(con, sprintf("CREATE TEMPORARY TABLE %s LIKE %s", staging_table, target_table))
  
  # Write only ins_cols; TEMP table may have extra cols (id) we won't use
  DBI::dbWriteTable(con, name = staging_table, value = payload, append = TRUE, row.names = FALSE)
  
  # Build update list (never update key cols, never includes auto-inc/generated)
  updatable <- setdiff(ins_cols, key_cols)
  set_clause <- paste(sprintf("%s = VALUES(%s)", updatable, updatable), collapse = ", ")
  
  # Upsert without auto-inc column
  sql <- sprintf(
    "INSERT INTO %1$s (%2$s)
       SELECT %2$s FROM %3$s
     ON DUPLICATE KEY UPDATE %4$s",
    target_table,
    paste(ins_cols, collapse = ", "),
    staging_table,
    set_clause
  )
  DBI::dbExecute(con, sql)
  
  DBI::dbExecute(con, sprintf("DROP TEMPORARY TABLE IF EXISTS %s", staging_table))
  DBI::dbCommit(con)
  on.exit(NULL)
  
  invisible(TRUE)
}

# ---- 3) Putting it together --------------------------------------------------
# Prepare data (vectorized) and upsert in one shot
payload <- prepare_current_payload(row_latest_goes)

# Choose the key(s) that make a row unique in klimoWBGT.current (adjust if needed)
# If 'current' is "one row per site", 'siteRecID' is likely the unique key.
bulk_upsert_current(payload, conn, target_table = "klimoWBGT.current", key_cols = c("siteRecID"))


# unique(row_latest_goes, by = 'siteRecID')



dbDisconnect(conn)




timed('end')

#
# 
# 
# range(row_latest_goes$ta)
# 
# 
# unique(row_latest_goes$ta)

###########################################################################
###########################################################################

# con <- dbConnect(RMySQL::MySQL(),   user = Sys.getenv('SQL_SERVER_USER'),
#                  password = Sys.getenv('SQL_SERVER_PW'),
#                  dbname = 'klimoWBGT',
#                  host = Sys.getenv("SQL_SERVER_IP2"),
#                  port = 3306,
#                  local_infile = TRUE)
#
# #########################################################################
# asq = getSQL2(sqlquery = 'select * from current limit 1', databaseName = 'klimoWBGT',
#              username  = Sys.getenv('SQL_SERVER_USER'),   pw  = Sys.getenv('SQL_SERVER_PW'),
#              serverip = Sys.getenv("SQL_SERVER_IP2"), as.datetime = FALSE)
# # asq$policyColor
#
# colnames(asq)[!colnames(asq) %in% colnames(row_latest_goes)]
#
# # stn$name
# # row_latest_goes$policyDescription = row_latest_goes$policyDetails
#
# cols2drop = colnames(row_latest_goes)[!colnames(row_latest_goes) %in% colnames(asq)]
# row_latest_goes = row_latest_goes[, !cols2drop, with  = FALSE]
# i = 1
# # row_latest_goes$tcdc
# # row_latest_goes = row_latest_goes[!is.na(validTime)]
# jj::timed('start')
# # stn[name == 'Shelby']$validTime
# # Iterate through each row in the data.table
# for (i in 1:nrow(row_latest_goes)) {
#   currec = row_latest_goes[i]
#
#   currec$wbgt = round(currec$wbgt)
#   currec$wbgt_sun = round(currec$wbgt_sun)
#   currec$wbgt_shade = round(currec$wbgt_shade)
#
#   # currec$wbgt_sun = round(currec$wbgt_sun)
#   # currec$tcdc = round(currec$tcdc)
#   currec$hi   = round(currec$hi)
#   currec$ta   = round(currec$ta)
#   currec$cloudcover   = round(as.numeric(currec$tcdc))
#
#   # currec$rh   = round(currec$rh)
#   # currec$td   = round(currec$td)
#   currec$wind2m = round(currec$wind2m)
#   # currec$wind10m = round(currec$wind10m)
#   currec$solar = round(currec$solar)
#   # currec$cloudcover = round(currec$cloudcover)
#   # currec$policyRecID  = assign_policy_api(currec$wbgt)
#   current_policy = assign_policy_api(currec$wbgt, currec$policyGroupID) # all_policies[policyRecID == currec$policyRecID][,c("policyDetails","policyName","policyRecID",'policyColor','policyPriority')]
#
#   # currec$policyRecID
#   if (current_policy$policyName == 'Clear Flag'){
#     currec$policyColor <- '#F8F8F8'
#   }
#   currec$policyDescription = current_policy$policyDetails
#
#   # Construct the update query with values directly
#   columns <- names(currec)
#
#   values <- sapply(currec, sanitize_value)
#
#   column_names <- paste(columns, collapse = ", ")
#   value_list <- paste(values, collapse = ", ")
#
#   set_clause <- paste(columns, "=VALUES(", columns, ")", collapse = ", ")
#
#   query <- paste0("INSERT INTO klimoWBGT.current (", column_names, ") VALUES (", value_list, ")
#                   ON DUPLICATE KEY UPDATE ", set_clause)
#
#   # Execute the query
#   dbExecute(con, query)
# }
#
# jj::timed('stop')
#
# dbDisconnect(con)
#
# #
#
# tmp <- tempfile(fileext = ".csv")
# fwrite(row_latest_goes, tmp, sep = ",", quote = TRUE, na = "\\N")   # \N = MySQL NULL
#
#
# con <- dbConnect(RMySQL::MySQL(),
#                  host     = Sys.getenv("SQL_SERVER_IP2"),  # Aurora host
#                  user     = Sys.getenv("SQL_SERVER_USER"),
#                  password = Sys.getenv("SQL_SERVER_PW"),
#                  dbname   = "klimoWBGT",
#                  port     = 3306,
#                  local_infile = TRUE)
#
# load_sql <- sprintf("
#   LOAD DATA LOCAL INFILE '%s'
#   INTO TABLE klimoWBGT.current
#   FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
#   LINES TERMINATED BY '\\n'
#   IGNORE 1 LINES
#   (%s)
#   ON DUPLICATE KEY UPDATE %s;
# ", tmp,
#                     paste(names(dt), collapse = ", "),
#                     paste(sprintf("%1$s = VALUES(%1$s)", names(dt)), collapse = ", "))
#
# dbExecute(con, load_sql)
# dbDisconnect(con)
# unlink(tmp)
#
#
#
#
#
#
# tmp_table <- "current_stage"
#
# # 1. Create stage table with identical structure
# dbExecute(con, sprintf("CREATE TEMPORARY TABLE %s LIKE klimoWBGT.current;", tmp_table))
#
# # 2. Bulk-load into the stage table
# load_sql <- sprintf("
#   LOAD DATA LOCAL INFILE '%s'
#   INTO TABLE %s
#   FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
#   LINES TERMINATED BY '\\n'
#   IGNORE 1 LINES
#   (%s);",
#                     tmp,
#                     tmp_table,
#                     paste(names(dt), collapse = ", ")
# )
# dbExecute(con, load_sql)
#
# # 3. Upsert from stage → target in a single statement
# cols <- names(dt)
# set_clause <- paste(sprintf("%1$s = VALUES(%1$s)", cols), collapse = ", ")
# dbExecute(
#   con,
#   sprintf("
#     INSERT INTO klimoWBGT.current (%s)
#     SELECT %s FROM %s
#     ON DUPLICATE KEY UPDATE %s;",
#           paste(cols, collapse = ", "),
#           paste(cols, collapse = ", "),
#           tmp_table,
#           set_clause
#   )
# )
#
# # 4. Temp table disappears when the session ends
#










