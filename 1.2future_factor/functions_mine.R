#the main function is packaged , to get the strategy return directly
get_strategy_return<-function(min_contract_number,threshold.1=4,threshold.2=4,turn_over_threshold=0.9,rebalance,factor){
if(factor=='term_structure'){
return(term_structure_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='momentum'){
return(momentum_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='value'){
return(value_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='volatility'){
return(volatility_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='OI'){
return(OI_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='liquidity'){
return(liquidity_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='beta_ex'){
return(beta_ex_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='beta_cpi'){
return(beta_cpi_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else if(factor=='skewness'){
return(skewness_strategy(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor))
 }else stop("rebalance cannot be identified.")
  }
# the individual 9 factor strategies are as follows, all the enviornment varibles and functions related are defined in tools_mine.R
#(1)Term structure------------------------------------------------------------------
term_structure_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){

code_long <- list()
code_short <- list()
dd<-256

trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)
tradelastday <- trade_code_space_datablock$date 
pre_mon<-which(tradelastday>= trade_date[(dd*0+1)])[1]+1


for(iter in pre_mon:Nloop){  # there are approx 120+ iter_s  say :first_month iter=2
  anchor <- which(trade_date==tradelastday[(iter-1)])  # they are last days ,+1 before using
  anchorend <- which(trade_date==tradelastday[iter])
   if (iter== (pre_mon)){anchor_init<-(anchor+1) }
  if (iter== (Nloop)){anchor_finl<-anchorend }
  print(iter)
  #tictoc::tic(paste("factor calculation on ", trade_date[anchor], sep=""))
  
  close_gap_trade_iter <- as.data.frame(lapply(1:length(close_gap),function(i){   #there are 72 i_s
    
    if(names(close_gap)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  )
    {
         #+1是因为date在第一列
      x <- close_gap[[i]]
    
      close_temp <- x[which(as.Date(x$date) %in% as.Date( trade_date[(anchor+1):anchorend])),]#该品种当月所有交易日对应的回报
  
      term_temp <- c()
      for(j in (anchor+1):anchorend)
        {
        n <- which(close_temp$date == trade_date[j])
        if(length(n)>1) stop("n is error.")
        ifelse(length(n)==1,term_temp <- c(term_temp,log(close_temp[n,2]/close_temp[n,3])),term_temp <- c(term_temp,NA))
      }
      return(term_temp)
    }
    else return(rep(NA,(anchorend-anchor)))
    
  }
  ))
  
  
  
  colnames(close_gap_trade_iter) <- names(close_gap)
  
  
	codes<-rank_and_select(trade_iter=close_gap_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[1:(length(codes)/2)])
  code_short<-c(code_short,codes[(length(codes)/2+1):length(codes)])
  
}
names(code_long)<-trade_date[anchor_init:anchor_finl]
names(code_short)<-trade_date[anchor_init:anchor_finl]

rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
}

#(2)Momentum------------------------------------------------------------------
momentum_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){

code_long <- list()
code_short <- list()
dd<-256

trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)
tradelastday <- trade_code_space_datablock$date 
pre_mon<-which(tradelastday>= trade_date[(dd*1+1)])[1]+1
#Nloop <-pre_mon+3
for(iter in pre_mon:Nloop){ 
  anchor <- which(trade_date==tradelastday[(iter-1)]) 
  anchorend <- which(trade_date==tradelastday[iter])
  if (iter== pre_mon){anchor_init<-(anchor+1) }
  if (iter== Nloop){anchor_finl<-anchorend }
  print(iter)
  
  mom_dom_trade_iter <- as.data.frame(lapply(1:length(dom_return),function(i){  
    
    if(names(dom_return)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  ){
      #+1 since date is the first col
      x <- dom_return[[i]]
      mom_temp <- c()
      wich<-which(as.Date(x$date) %in% as.Date( trade_date[(anchor+1)]))
      if (length(wich)==1)
      {
      if (wich>=dd)# if history data is more than 256 days
      {
        for(j in (anchor+1):anchorend)
        {
          n <- which(x$date == trade_date[j])
          if(length(n)>1) stop("n is error.")
          ifelse(length(n)==1,mom_temp <- c(mom_temp,sum(x$value[(n-dd+1):n],na.rm=TRUE)/dd),mom_temp <- c(mom_temp,NA))
        }
        return(mom_temp)
      }
      
      else return(rep(NA,(anchorend-anchor)))
    }
    else return(rep(NA,(anchorend-anchor)))
    }
    else return(rep(NA,(anchorend-anchor)))
    
  }))
  
  
  
  colnames(mom_dom_trade_iter) <- names(dom_return)
	codes<-rank_and_select(trade_iter=mom_dom_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[1:(length(codes)/2)])
  code_short<-c(code_short,codes[(length(codes)/2+1):length(codes)])
  
}
names(code_long)<-trade_date[anchor_init:anchor_finl]
names(code_short)<-trade_date[anchor_init:anchor_finl]
rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
}

#(3)Value------------------------------------------------------------------
value_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){

code_long <- list()
code_short <- list()
dd<-256


trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)
tradelastday <- trade_code_space_datablock$date 
pre_mon<-which(tradelastday>= trade_date[(dd*5.5+1)])[1]+1

for(iter in pre_mon:Nloop){ 
  anchor <- which(trade_date==tradelastday[(iter-1)]) 
  anchorend <- which(trade_date==tradelastday[iter])
  if (iter== pre_mon){anchor_init<-(anchor+1) }
  if (iter== Nloop){anchor_finl<-anchorend }
  print(iter)
  
   value_close_trade_iter <- as.data.frame(lapply(1:length(close_gap),function(i){   #there are 72 i_s
    
    if(names(close_gap)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  )
      {
     
      x <- close_gap[[i]]
      value_temp <- c()
      wich<-which(as.Date(x$date) %in% as.Date( trade_date[(anchor+1)]))
      if (length(wich)==1)
      {
      if (wich>=dd*5.5)
      {
      for(j in (anchor+1):anchorend)
      {
        n <- which(x$date == trade_date[j])
        if(length(n)>1) stop("n is error.")
        ifelse(length(n)==1,value_temp <- c(value_temp,log(sum(x[(n-dd*5.5+1):(n-dd*4.5),2],na.rm=TRUE)/dd/x[n,2])),value_temp <- c(value_temp,NA))
      }
      return(value_temp)
      }
      else return(rep(NA,(anchorend-anchor)))
    }
    else return(rep(NA,(anchorend-anchor)))
    }
    else return(rep(NA,(anchorend-anchor)))
  }))
  
  
  
  
  colnames(value_close_trade_iter) <- names(close_gap)
	codes<-rank_and_select(trade_iter=value_close_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[1:(length(codes)/2)])
  code_short<-c(code_short,codes[(length(codes)/2+1):length(codes)])
  
}
names(code_long)<-trade_date[anchor_init:anchor_finl]
names(code_short)<-trade_date[anchor_init:anchor_finl]
rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
}

#(4)CV_monthly------------------------------------------------------------------
volatility_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){
code_long <- list()
code_short <- list()
dmon<-36


trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)
tradelastday <- trade_code_space_datablock$date 




cv_dom_trade_iter <- as.data.frame(lapply(1:length(dom_return),function(i){   #there are 72 i_s
  cv_temp <- c()
  for(iter in dmon:Nloop)
  {
    
    if(names(dom_return)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  ){
    
      x <- dom_return_monthly[,i+1]
      
      
      
      if (length(which(is.na(x[(iter-dmon+1):iter])==FALSE))==dmon)#if the past  36-month returns exist
      {
        cv_temp <- c(cv_temp,var(x[(iter-dmon+1):iter])/abs(mean(x[(iter-dmon+1):iter])))
      }
      
      else cv_temp <- c(cv_temp,NA)
      
      
    }
    else cv_temp <- c(cv_temp,NA)
  }
  return (cv_temp)
  
}))
#since the result dataframe is exact (rank targets of all months) we keep it and renamed
cv_dom_trade_iter_named <- cbind(dom_return_monthly$date[dmon:Nloop],cv_dom_trade_iter)
colnames(cv_dom_trade_iter_named) <- colnames(dom_return_monthly)

  colnames(cv_dom_trade_iter) <- names(dom_return)
  codes<-rank_and_select(trade_iter=cv_dom_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[1:(length(codes)/2)])
  code_short<-c(code_short,codes[(length(codes)/2+1):length(codes)])
  names(code_long)<-dom_return_monthly$date[dmon:Nloop]
  names(code_short)<-dom_return_monthly$date[dmon:Nloop]
rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
}

#(5)OI_monthly------------------------------------------------------------------
OI_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){
code_long <- list()
code_short <- list()
dmon<-2


trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)
tradelastday <- trade_code_space_datablock$date 




oi_dom_trade_iter <- as.data.frame(lapply(1:length(dom_return),function(i){   #there are 72 i_s
  oi_temp <- c()
  for(iter in dmon:Nloop)
  {
    
    if(names(dom_return)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  ){
     
      x <- oi_monthly[,i+1]
      
      
      
      if ((is.na(x[iter-1])==FALSE)&(is.na(x[iter])==FALSE))
      {
        oi_temp<- c(oi_temp,(x[iter]-x[iter-1])/x[iter-1])
      }
      
      else oi_temp<- c(oi_temp,NA)
      
      
    }
    else oi_temp <- c(oi_temp,NA)
  }
  return (oi_temp)
  
}))

#since the result dataframe is exact (rank targets of all months) we keep it and renamed
oi_dom_trade_iter_named <- cbind(oi_monthly$date[dmon:Nloop],oi_dom_trade_iter)
colnames(oi_dom_trade_iter_named) <- colnames(oi_monthly)

  colnames(oi_dom_trade_iter) <- names(dom_return)
  codes<-rank_and_select(trade_iter=oi_dom_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[1:(length(codes)/2)])
  code_short<-c(code_short,codes[(length(codes)/2+1):length(codes)])
  names(code_long)<-oi_monthly$date[dmon:Nloop]
  names(code_short)<-oi_monthly$date[dmon:Nloop]
rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
}
#(6)Liquidity------------------------------------------------------------------

liquidity_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){
code_long <- list()
code_short <- list()
dd<-42

trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)
tradelastday <- trade_code_space_datablock$date 
pre_mon<-which(tradelastday>= trade_date[(dd+1)])[1]+1

for(iter in pre_mon:Nloop){ 
  anchor <- which(trade_date==tradelastday[(iter-1)]) 
  anchorend <- which(trade_date==tradelastday[iter])
  if (iter== pre_mon){anchor_init<-(anchor+1) }
  if (iter== Nloop){anchor_finl<-anchorend }
  print(iter)
  
  lq_dom_trade_iter <- as.data.frame(lapply(1:length(dom_return),function(i){  
    
    if(names(dom_return)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  ){
      #+1 since date is the first col
      x <- dom_return[[i]]
      y <- data_vol_oi[[i]]
      lq_temp <- c()
      # take care of the date format of data_vol_oi
      wich<-which(as.Date(x$date) %in% as.Date( trade_date[(anchor+1)]))
      if (length(wich)==1)
      {
      if (wich>=dd)# if history data is more than 42 days
      {
        for(j in (anchor+1):anchorend)
        {
          n <- which(x$date == trade_date[j])
          ny <- which(as.Date(y$date) == trade_date[j])# the date sequential of y may not equal that of x's
          if(length(n)>1) stop("n is error.")
          # if divided by zero
          ifelse(length(n)==1,lq_temp <- c(lq_temp,sum(is.finite(y$total_turnover[(ny-dd+1):ny]/x$value[(n-dd+1):n])*(1e-10)*y$total_turnover[(ny-dd+1):ny]/abs(x$value[(n-dd+1):n]),na.rm=TRUE)/dd),lq_temp <- c(lq_temp,NA))
        }
        return(lq_temp)
      }
      
      else return(rep(NA,(anchorend-anchor)))
      
      
    }
    else return(rep(NA,(anchorend-anchor)))
    }
    else return(rep(NA,(anchorend-anchor)))
  }))
  
  
  
  colnames(lq_dom_trade_iter) <- names(dom_return)
	codes<-rank_and_select(trade_iter=lq_dom_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[(length(codes)/2+1):length(codes)])# note that liquidity should be used  inversely
  code_short<-c(code_short,codes[1:(length(codes)/2)])  
}
names(code_long)<-trade_date[anchor_init:anchor_finl]
names(code_short)<-trade_date[anchor_init:anchor_finl]

rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
}
#(7)Beta of exchange_rate_monthly------------------------------------------------------------------

beta_ex_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){
code_long <- list()
code_short <- list()
dmon<-60


trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)
tradelastday <- trade_code_space_datablock$date 




ex_dom_trade_iter <- as.data.frame(lapply(1:length(dom_return),function(i){   #there are 72 i_s
  ex_temp <- c()
  for(iter in dmon:Nloop)
  {
    
    if(names(dom_return)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  ){
    
      x <- dom_return_monthly[,i+1]
      y <- ex_mon[-1,3]# omit 1st ,now the date range of x and y is equal
      
      
      
      if (length(which(is.na(x[(iter-dmon+1):iter])==FALSE))==dmon)#if the past  60-month returns exist
      {
      	fit<-lm(x[(iter-dmon+1):iter]~y[(iter-dmon+1):iter])
        ex_temp <- c(ex_temp,fit$coefficients[[2]])
      }
      
      else ex_temp <- c(ex_temp,NA)
      
      
    }
    else ex_temp <- c(ex_temp,NA)
  }
  return (ex_temp)
  
}))
#since the result dataframe is exact (rank targets of all months) we keep it and renamed
ex_dom_trade_iter_named <- cbind(dom_return_monthly$date[dmon:Nloop],ex_dom_trade_iter)
colnames(ex_dom_trade_iter_named) <- colnames(dom_return_monthly)

  colnames(ex_dom_trade_iter) <- names(dom_return)
  codes<-rank_and_select(trade_iter=ex_dom_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[(length(codes)/2+1):length(codes)])# note that ex beta should be used  inversely
  code_short<-c(code_short,codes[1:(length(codes)/2)])  
  names(code_long)<-dom_return_monthly$date[dmon:Nloop]
  names(code_short)<-dom_return_monthly$date[dmon:Nloop]
rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
}
#(8)Beta of cpi_monthly------------------------------------------------------------------

beta_cpi_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){
code_long <- list()
code_short <- list()
dmon<-60


trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)-1 #-1 since the last data value of cpi is not available
tradelastday <- trade_code_space_datablock$date 




cpi_dom_trade_iter <- as.data.frame(lapply(1:length(dom_return),function(i){   #there are 72 i_s
  cpi_temp <- c()
  for(iter in dmon:Nloop)
  {
    
    if(names(dom_return)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  ){
    
      x <- dom_return_monthly[,i+1]
      
      y <- cpi_monthly[which(cpi_monthly$date>=dom_return_monthly$date[1]),2]      
      
      
      if (length(which(is.na(x[(iter-dmon+1):iter])==FALSE))==dmon)#if the past  60-month returns exist
      {
      	fit<-lm(x[(iter-dmon+1):iter]~y[(iter-dmon+1):iter])
        cpi_temp <- c(cpi_temp,fit$coefficients[[2]])
      }
      
      else cpi_temp <- c(cpi_temp,NA)
      
      
    }
    else cpi_temp <- c(cpi_temp,NA)
  }
  return (cpi_temp)
  
}))
#since the result dataframe is exact (rank targets of all months) we keep it and renamed
cpi_dom_trade_iter_named <- cbind(dom_return_monthly$date[dmon:Nloop],cpi_dom_trade_iter)
colnames(cpi_dom_trade_iter_named) <- colnames(dom_return_monthly)

  colnames(cpi_dom_trade_iter) <- names(dom_return)
  codes<-rank_and_select(trade_iter=cpi_dom_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
    code_long<-c(code_long,codes[1:(length(codes)/2)])
  code_short<-c(code_short,codes[(length(codes)/2+1):length(codes)])
  names(code_long)<-dom_return_monthly$date[dmon:Nloop]
  names(code_short)<-dom_return_monthly$date[dmon:Nloop]
 rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
} 
  
#(9)Skewness------------------------------------------------------------------

skewness_strategy<-function(min_contract_number=min_contract_number,threshold.1=threshold.1,threshold.2=threshold.2,turn_over_threshold=turn_over_threshold,rebalance=rebalance,factor=factor){
code_long <- list()
code_short <- list()
dd<-256

trade_code_space_datablock <- trade_code_space(turnover_monthly_rank=turnover_monthly_rank,turnover_monthly_rank_date=turnover_monthly_rank_date,return_gap_continue=return_gap_continue,turn_over_threshold=turn_over_threshold,min_contract_number=min_contract_number)
Nloop <- nrow(trade_code_space_datablock)

tradelastday <- trade_code_space_datablock$date 
pre_mon<-which(tradelastday>= trade_date[(dd+1)])[1]+1
#Nloop<-pre_mon+2
for(iter in pre_mon:Nloop){ 
  anchor <- which(trade_date==tradelastday[(iter-1)]) 
  anchorend <- which(trade_date==tradelastday[iter])
  if (iter== pre_mon){anchor_init<-(anchor+1) }
  if (iter== Nloop){anchor_finl<-anchorend }
  print(iter)
  
  
  skew_dom_trade_iter <- as.data.frame(lapply(1:length(dom_return),function(i){  
    
    if(names(dom_return)[i] %in% colnames(trade_code_space_datablock)[which(trade_code_space_datablock[(iter-1),-1]==1)+1]  ){
      #+1 since date is the first col

      x <- dom_return[[i]]
      skew_temp <- c()
      
      wich<-which(as.Date(x$date) %in% as.Date( trade_date[(anchor+1)]))
      if (length(wich)==1)
      {
      if (wich>=dd)# if history data is more than 256 days
      {
        for(j in (anchor+1):anchorend)
        {
          n <- which(x$date == trade_date[j])
          if(length(n)>1) stop("n is error.")
          ifelse(length(n)==1,skew_temp <- c(skew_temp,skewness(x$value[(n-dd+1):n],na.rm=TRUE)),skew_temp <- c(skew_temp,NA))
        }
        return(skew_temp)
      }
      
      else return(rep(NA,(anchorend-anchor)))
      
      
    }
    else return(rep(NA,(anchorend-anchor)))
    }
    else return(rep(NA,(anchorend-anchor)))
  }))
  
  
  
  colnames(skew_dom_trade_iter) <- names(dom_return)
	codes<-rank_and_select(trade_iter=skew_dom_trade_iter,threshold.1=threshold.1,threshold.2=threshold.2)
  code_long<-c(code_long,codes[(length(codes)/2+1):length(codes)])# note that skewness should be used  inversely
  code_short<-c(code_short,codes[1:(length(codes)/2)])  
}
names(code_long)<-trade_date[anchor_init:anchor_finl]
names(code_short)<-trade_date[anchor_init:anchor_finl]
 rlt<-strategy_return(dom_return_daily=dom_return_daily,code_long=code_long,code_short=code_short,rebalance=rebalance,factor=factor)
return (rlt)
} 