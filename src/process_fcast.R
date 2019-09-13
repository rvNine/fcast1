# Big Time Series forecasting ----

rm(list = ls())
gc()

library(tidyverse)
library(data.table)
library(foreach)
library(lubridate)
library(parallel)
library(doSNOW)
library(Metrics)
library(forecast)
library(prophet)
library(bigstatsr)
library(plyr)

# 1.0 Prepare train/test split ----

## reading data file and prepating training and validation set


## reading train data 
data_full <- fread('../data/raw/train.csv', skip = 36458909, 
               col.names = c('id', 'date','store_nbr', 'item_nbr', 'unit_sales', 'onpromotion'))




data_by_store_nbr_top5_sales <- data_full %>% 
        dplyr::group_by(store_nbr)%>% 
        dplyr::summarize(total_sales = sum(unit_sales)) %>% 
        dplyr::arrange(desc(total_sales)) %>%
        dplyr::top_n(5)

#Top 5 stores by sales
data_by_store_nbr_top5_sales$store_nbr

data <- data_full[store_nbr %in%  data_by_store_nbr_top5_sales$store_nbr]


data %>% 
  dplyr::distinct(store_nbr)

rm(data_full)

data[, ':='(
  date = ymd(date, tz = NULL),
  store_item = paste(store_nbr, item_nbr, sep="_")
)]

data_wide <- dcast(data, store_item ~ date, value.var = "unit_sales", fill = 0)

date_index_test <- tail(colnames(data_wide), 16)


## train model ----
train_model <- function(i, model_type = "mean", data_wide = NULL) {
    

  if (model_type == "mean") {
    .call <- parse(text = "meanf(x = train_ts, h = h)")
  } else if (model_type == "ets") {
    .call <- parse(text = "ets(y = train_ts)")
  } else if (model_type == "auto.arima") {
    .call <- parse(text = "auto.arima(x = train_ts)")
  } else if (model_type == "prophet") {
    .call <- parse(text = "prophet(df = train_data, yearly.seasonality=TRUE, weekly.seasonality = TRUE,
                 daily.seasonality=FALSE)")
  } else {
    stop(sprintf("Unknown model '%s'!", model_type))
  }
  
  data_df <- melt(data_wide[i,], id.vars = c("store_item"))
  
  store_item_val <- data_df$store_item[1]
  data_df$store_item <- NULL
  
  colnames(data_df) <- c("ds", "y")
 
  # Date column handling
  data_ts <- data_df
  data_ts <- data_ts %>% 
                select(-ds)

  h <- 16
  frequency <- 7
  test_index <- tail(rownames(data_ts), h) %>% as.numeric()
  test_data <- data_ts %>% slice(test_index) %>% `row.names<-`(test_index)
  train_data <- data_ts %>% slice(-test_index)
  
  # Converts train data to time.series object
  train_ts <- ts(train_data, frequency = frequency, start = 1)
  column_name_i <- store_item_val
    
  # Trains model
  if (model_type == "mean") {
    fit_i <- eval(.call)
    
    t_data <- test_data$y %>% as.numeric()
    
    accuracy_i <- forecast::accuracy(fit_i, t_data)[2, ]
    
    fcast_i <-  as.numeric(fit_i$mean)
    
  } else if (model_type == "prophet") {
    
    
    test_index <- tail(rownames(data_df), h) %>% as.numeric()
    test_data <- data_df %>% slice(test_index) %>% `row.names<-`(test_index)
    train_data <- data_df %>% slice(-test_index)
   
    
    df = train_data
    yearly.seasonality=TRUE
    weekly.seasonality = TRUE
    daily.seasonality=FALSE
    
    fit_i <- eval(.call)
    future <- make_future_dataframe(fit_i, periods = 16)
    forecast <- predict(fit_i, future)
    forecast <- forecast[c('ds', 'yhat')]
    forecast$store_item <- store_item_val

    forecast <- forecast[forecast$ds >= ymd("2017-07-30"),]
    
    t_data <- test_data$y %>% as.numeric()
    accuracy_i <- forecast::accuracy(forecast$yhat, t_data)
    
    fcast_i <-  as.numeric(forecast$yhat)
    
  } else {
    fit_i <- eval(.call)
    # Makes forecast and calculates accuracy measures
    forecast_i <- forecast(fit_i, h = h)

    t_data <- test_data$y %>% as.numeric()
    accuracy_i <- forecast::accuracy(forecast_i, t_data)[2, ]
    
    fcast_i <-  as.numeric(forecast_i$mean)
  }
  
  data_frame(fcast = list(fcast_i), column_names = list(column_name_i), out_accuracy = list(accuracy_i))
    
}
   
  

generate_results <- function(model_type = "mean", data_wide = NULL ) {
  results <- data.frame(NULL)
  
  n_cores <- bigstatsr::nb_cores()
  
  cl <- makeCluster(n_cores)
  registerDoSNOW(cl)
  
  start_p <- Sys.time()
  number_of_iterations <- nrow(data_wide)
  
  
  if (model_type == "prophet") {
    packages_l <- c("prophet", "forecast", 'data.table', 'lubridate', 'tidyverse', 'magrittr', 'dplyr')
  } else {
    packages_l <- c("forecast", 'data.table', 'lubridate', 'tidyverse', 'magrittr', 'dplyr')
  }
 
  
  results <- foreach(i = 1:number_of_iterations, .verbose = TRUE,
                   .packages = packages_l, .export = "train_model",
                   .combine='rbind') %dopar% {
                     results <- train_model(i, model_type = model_type, data_wide = data_wide )  
                     results
                   }
  end_p <- Sys.time()

  stopCluster(cl)


  out_accuracy <-
    ldply(results$out_accuracy) %>%
    magrittr::set_rownames(results$column_names)
  head(out_accuracy)
  
  # Extract point forecasts
  fcast <-
    ldply(results$fcast) %>%
    magrittr::set_rownames(results$column_names) %>%
    magrittr::set_colnames(date_index_test) %>%
    as.data.frame()
  
  
  # Measures time for calculations----------------------------------------------
  diff_time <- end_p - start_p
  duration_f <- sprintf("%0.2f %s", diff_time, attr(diff_time, "units"))
  diff_time_mean_rmse <- list(mean_rmse = mean(out_accuracy$RMSE),
                              time_taken = duration_f)
  dfname <- paste(model_type, "train_duration_mean_rmse", sep = "_")
  write.csv(diff_time_mean_rmse, file = dfname, row.names = FALSE)
  

  fname <- paste(model_type, "accuracy.csv", sep = "_")
  write.csv(out_accuracy, file=fname)
  
  fcastname <- paste(model_type, "fcast.csv", sep = "_")
  write.csv(fcast, file=fcastname)

}

generate_results(model_type = "mean", data_wide = data_wide )
generate_results(model_type = "ets" , data_wide = data_wide )
generate_results(model_type = "auto.arima", data_wide = data_wide )
generate_results(model_type = "prophet", data_wide = data_wide )