#(1):library loading-------------------------

library('e1071')#load skewness()
library(xts)#ts_plot

#(2):data loading(by .Rdata)-------------------------
#basic_datablock
#data_vol2 #data_vol_oi shouled be ordered by names of dom_return and shares the same date range before using
#cpi_monthly
#exchange_rate
load("/Users/sexin/Desktop/htf_code/2020_annualreport/basic_datablock.RData")
load("/Users/sexin/Desktop/htf_code/2020_annualreport/data_vol2.RData")
load("/Users/sexin/Desktop/htf_code/2020_annualreport/cpi_monthly.RData")
load("/Users/sexin/Desktop/htf_code/2020_annualreport/exchange_rate.RData")
turnover_monthly_rank<-basic_datablock$turnover_monthly_rank
turnover_monthly_rank_date<-basic_datablock$turnover_monthly_rank_date
trade_date<-basic_datablock$trade_date
return_gap_continue<-basic_datablock$return_gap_continue
close_gap<-basic_datablock$close_gap 
# symbol_gap<-basic_datablock$symbol_gap
# basic_datablock$dom_data_all
dom_return<-basic_datablock$dom_return

#tradelastday0 will be calculated after LastDay is defined

#(3):define my_functions-------------------------

#-------------------------month LastDay-------------
LastDay <- function(tradeday,enable_currentmonth = FALSE, frequency= c("day","week","month")[1]) {
  Nday <- length(tradeday)
  
  if(frequency=="month"){
    dayvect <- as.numeric(unlist(strsplit(tradeday, "[-]")))
    dayvect <- matrix(dayvect,nrow=3)
    
    tradelastday <- c()
    position <- c()
    for (i in 2:Nday) {
      if (dayvect[2,i-1] != dayvect[2,i]) {
        tradelastday <- c(tradelastday,tradeday[i-1])
        position <- c(position, i-1)
      }
      if(enable_currentmonth == TRUE){
        if (i==Nday) {
          tradelastday <- c(tradelastday,tradeday[Nday])
          position <- c(position, Nday)
        }
      }
    }
  }else if(frequency=="week"){

    weekvect <- format(as.Date(tradeday) ,'%w') 
    
    tradelastday <- c()
    position <- c()
    for (i in 2:Nday) {
      if (weekvect[i-1]>weekvect[i]) {
        tradelastday <- c(tradelastday,tradeday[i-1])
        position <- c(position, i-1)
      }
      if(enable_currentmonth == TRUE){
        if (i==Nday) {
          tradelastday <- c(tradelastday,tradeday[Nday])
          position <- c(position, Nday)
        }
      }
    }
  }else if(frequency=="day"){

    tradelastday <- tradeday
    position <- c(1:Nday)
  }else stop("frequency cannot be identified.")
  
  return( list("tradelastday"=tradelastday,"position"=position) )
}

tradelastday0 <- (LastDay(trade_date,enable_currentmonth = TRUE, frequency= "month"))$tradelastday#------------------------

#trade_code_space----------------------------------------------------------

trade_code_space <- function(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=0.6,min_contract_number=3){
  
  trade_code_list_temp <- lapply(1:length(turnover_monthly_rank_date),function(i){
    x<- turnover_monthly_rank[i,]
    # x[which(x>=turn_over_threshold)] <- NA
    
    
    if(is.null(min_contract_number)==FALSE){
      return_gap_continue <- unlist(lapply(return_gap_continue,function(y){
        # browser()
        ifelse(i>1,y <- subset(y,date>turnover_monthly_rank_date[i-1]&date<=turnover_monthly_rank_date[i]),y <- subset(y,date>tradelastday0[1]&date<=turnover_monthly_rank_date[i]))
        
        symbol_num <- apply(y,1,function(z)return(length(which(is.na(z)==FALSE))))
        if(is.null(symbol_num)==FALSE&min(unlist(symbol_num))>=(min_contract_number+1) )return(1)
        else return(0)
      }))
    }else{
      return_gap_continue <- unlist(lapply(return_gap_continue,function(y){return(1)}))
    }
    
    
    code_i <- intersect(names(return_gap_continue)[which(return_gap_continue==1)],names(x)[which(x<turn_over_threshold)])
    y <- rep(NA,ncol(turnover_monthly_rank))
    
    y[which(colnames(turnover_monthly_rank) %in% code_i)] <-1
    # x[which(is.na(x)==FALSE)] <-NA
    return(y)
  })
  
  
  trade_code_list_temp <- matrix(unlist(trade_code_list_temp),nrow=length(turnover_monthly_rank_date),byrow=TRUE)
  
  
  trade_code_datablock <-as.data.frame(matrix(numeric(0),ncol=ncol(trade_code_list_temp)+1,nrow = nrow(trade_code_list_temp) ))
  trade_code_datablock[,1] <- turnover_monthly_rank_date
  trade_code_datablock[,-1] <- trade_code_list_temp
  colnames(trade_code_datablock) <- c("date",colnames(turnover_monthly_rank))
  
  return(trade_code_datablock)
}

#------------timeserise to dataframe c("date","value")---------------
ts_to_df <- function(date,value){
  date <- as.character(date)
  value <- as.numeric(value)
  if(length(date)!=length(value)) stop("length(date)!=length(value)")
  dataframe <- as.data.frame(matrix(numeric(0),ncol=2,nrow = length(date) ))
  colnames(dataframe) <- c("date","value")
  dataframe$date <- date
  dataframe$value <- value
  return(dataframe)
}
#-----------------plot_ts--------------
plot_ts <- function(date,return){
  cumreturn <- xts::xts(cumprod(1+return),order.by = as.Date(date))
  name<-plot(cumreturn, type="l",main="return", ylab="return",lwd=2,cex.axis=1.5,cex.main=1.5)
  #browser()
  print(name)
}

#rank_and_select-----------------------------------------------------------
rank_and_select<-function(trade_iter,threshold.1,threshold.2){


  strategy_rank=t(apply(trade_iter,1,function(x){
    temp <- rank(x,na.last = TRUE)/length(which(is.na(x)==FALSE))
    temp[which(temp > 1)] <- NA
    return(temp)}))

colnames(strategy_rank) <- names(trade_iter)

code_long <- list()
code_short <- list()

for(k in 1:nrow(strategy_rank)){
	if (threshold.1>=1) # imply interger
	{
code_long_i <- colnames(strategy_rank)[which(strategy_rank[k,]>=sort(strategy_rank[k,], decreasing = TRUE)[[threshold.1]])]
code_short_i <-colnames(strategy_rank)[which(strategy_rank[k,]<=sort(strategy_rank[k,], decreasing = FALSE)[[threshold.2]])]
	}
	else 
	{
  code_long_i <- colnames(strategy_rank)[which(strategy_rank[k,]>threshold.1)]
  code_short_i <- colnames(strategy_rank)[which(strategy_rank[k,]<threshold.2)]
    }
  code_long <- c(code_long , list(code_long_i))
  code_short <- c(code_short , list(code_short_i))
}
return (c(code_long ,code_short))
}
   
#make_portfolio(rebalanced)-----------------------------------------------------
strategy_return<-function(dom_return_daily,code_long,code_short,rebalance,factor){
	trade_date<-dom_return_daily$date
	
  if(rebalance=="month"){
    ltradeday <- LastDay(trade_date, enable_current = TRUE, frequency= rebalance)
    tradelastday <- ltradeday$tradelastday
  }else if(rebalance == "day"){
    ltradeday <- LastDay(trade_date, enable_current = TRUE, frequency= rebalance)
    tradelastday <- ltradeday$tradelastday
  }else if(rebalance=="week"){
    ltradeday <- LastDay(trade_date, enable_current = TRUE, frequency= rebalance)
    tradelastday <- ltradeday$tradelastday
  }else stop("rebalance cannot be identified.")
  

strategy_return<-c()
strategy_trade_date<-c()
Nloop <- length(tradelastday)
pre_mon<-which(tradelastday>= names(code_long)[1])[1]+1


for(iter in pre_mon:Nloop){  # there are approx 120+ iter_s  say :first_month iter=2
  anchor <- which(trade_date==tradelastday[(iter-1)])  # they are last days ,+1 before using
  anchorend <- which(trade_date==tradelastday[iter])
  # if (iter== (pre_mon)){anchor_init<-(anchor+1) }
 # if (iter== (Nloop)){anchor_finl<-anchorend }
  
return_long<-dom_return_daily[(anchor+1):anchorend,code_long[[which(names(code_long)==tradelastday[(iter-1)])]]]
return_short<-dom_return_daily[(anchor+1):anchorend,code_short[[which(names(code_short)==tradelastday[(iter-1)])]]]

return_tmp <- cbind(return_long,(-1*return_short))

weight_init <- return_tmp[1,]/return_tmp[1,]/length(which(is.na(return_tmp)==FALSE))
    
    wights_iterw <- weight_init
    strategy_return_iterd<- c()
    ## recalculate weight everyday
    for (iterd in 1:(anchorend-anchor)) {
      strategy_return_iterd <- c(strategy_return_iterd, sum(wights_iterw * return_tmp[iterd,],na.rm = TRUE) )
      # wights_iterw <- rbind(wights_temp,temp_weight)##
      wights_iterw <- wights_iterw * (1+return_tmp[iterd,])
      wights_iterw <- wights_iterw / sum(wights_iterw,na.rm = TRUE)
    }
    
strategy_return <- c(strategy_return,strategy_return_iterd)
strategy_trade_date <- c(strategy_trade_date ,trade_date[(anchor+1):anchorend])  
  
}
  strategy_return_ts <- ts_to_df(date=strategy_trade_date,value=strategy_return)
  plot_ts(strategy_return_ts$date,strategy_return_ts$value)
  file_name<-paste("/Users/sexin/Desktop/htf_code/2020_annualreport/2020_annualreport/datas/",factor,"_",rebalance,".RData", sep = "", collapse = NULL)
  #varible_name<-paste(factor,"_",rebalance,"_return",".RData", sep = "", collapse = NULL)
  save(strategy_return_ts,file=file_name) # note that all the varible names will be the same, namely "strategy_return_ts"
  return(strategy_return_ts)
  
}


#(4):prepare my_data-------------------------

#dom_return_daily(data.frame)--------------------the missing daily return is filled with NA--------------------
dom_return_daily <- as.data.frame(lapply(dom_return,function(x){
  anchor<-which(trade_date==min(as.Date(x$date)))
  if(max(as.Date(x$date))>max(as.Date(trade_date)))
  {anchorend<-length(trade_date)}
  if(max(as.Date(x$date))<=max(as.Date(trade_date)))
 {anchorend<-which(trade_date==max(as.Date(x$date)))}
 
  day_x <- c(rep(NA,anchor-1))
  for(i in anchor:anchorend)
        {
        n <- which(x$date == trade_date[i])
        if(length(n)>1) stop("n is error.")
        ifelse(length(n)==1,day_x <- c(day_x,x$value[n]),day_x <- c(day_x,NA))
      }
    day_x <- c(day_x,rep(NA,length(trade_date)-anchorend))
  return(day_x)
}))

dom_return_daily <- cbind(trade_date,dom_return_daily)

colnames(dom_return_daily) <- c('date',names(dom_return)) #now ,the date range is (2010-01-04,2020-09-08)

#dom_return_monthly(data.frame)-----------------the missing daily return is filled with NA-----------------------
dom_return_monthly <- as.data.frame(lapply(dom_return,function(x){
  
  print(min(as.Date(x$date)))
  tradelastday_x <- subset(tradelastday0,tradelastday0>min(as.Date(x$date)))
  month_x <- c(rep(NA,(length(tradelastday0)-length(tradelastday_x)+1)))
  for(i in 2:length(tradelastday_x)){
    month_i <- unlist(subset(x,date>tradelastday_x[i-1]&date<=tradelastday_x[i],value))
    month_x <- c(month_x,ifelse(length(month_i)==0,NA,prod(month_i+1,na.rm = TRUE)-1))
  }
  return(month_x[-1])#drop the start month return
}))
#drop the start month return 'but' keep the end month return
dom_return_monthly <- cbind(tradelastday0[-1],dom_return_monthly)


colnames(dom_return_monthly) <- c('date',names(dom_return)) #now ,the date range is (2010-02-26,2020-09-08)

#oi_monthly(data.frame)------------------------------------------------------------------

oi_monthly <- as.data.frame(lapply(data_vol_oi,function(x){# take care of the date format
  
  print(min(as.Date(x$date)))
  tradelastday_x <- subset(tradelastday0,tradelastday0>min(as.Date(x$date)))
  oi_x <- c(rep(NA,(length(tradelastday0)-length(tradelastday_x)+1)))
  for(i in 2:length(tradelastday_x)){
    oi_i <- unlist(subset(x,date>tradelastday_x[i-1]&date<=tradelastday_x[i],open_interest))
    oi_x <- c(oi_x,ifelse(length(oi_i)==0,NA,sum(oi_i)))
  }
  return(oi_x[-1])#drop the start month return
}))
#omit the start month return 'but' keep the end month return
oi_monthly <- cbind(tradelastday0[-1],oi_monthly)
colnames(oi_monthly) <- c('date',names(data_vol_oi))

#exchange rate monthly(data.frame)------------------------------------------------------------------
ex_mon<-exchange_rate[which(as.Date(exchange_rate$date) %in% as.Date(tradelastday0)),]
ex_mon<-cbind(ex_mon,rep(0,nrow(ex_mon)))
names(ex_mon)<-c('date','rate','per')
for (i in 2:nrow(ex_mon)){ex_mon[i,3]<-((ex_mon[i,2]-ex_mon[(i-1),2])/ex_mon[(i-1),2])}
#now ,the date range is (2010-01-26,2020-09-08) where the 'per'value of 2010-01-26 isn't  assigned 

