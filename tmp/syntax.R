# R syntax
library(rio)
library(dplyr)
library(tidyselect)
library(tidyr)
library(purrr)
library(stringr)
library(tictoc)
tic("Merge")
# set working directory
wd = 'C:/Users/rjbischo/Nextcloud/CatMapper_nc/merge_syntax'
setwd(wd)
if(!dir.exists('temp'))
  dir.create('temp')
# load categories used in merge

categories = rio::import("categories.xlsx", col_types = "text")

data = rio::import("data.xlsx", col_types = "text")

template = data %>% 
  select(mergingID, stackID, datasetID, datasetName, filePath) %>% 
  distinct_all()

# template = rio::import('template.csv') %>% dplyr::mutate_all(as.character)
# categories = rio::import('mergingCategories.csv') %>% dplyr::mutate_all(as.character) %>%
#   dplyr::mutate_all(as.character) %>% tidyr::separate_rows(datasetID, sep=" \\|\\| ") %>%
#   dplyr::mutate_all(stringr::str_trim)
# metadata = rio::import('metadata.csv') %>% dplyr::mutate_all(as.character)
# variables = rio::import('variables.csv')
# stackVariables = tryCatch(rio::import('stackVariables.csv'),error = function(e)return(tibble::tibble()))

# Functions
CM2list = function(l){
  l %>% unlist %>% unique %>% sort %>% .[which(!is.na(.))] %>% paste(collapse = "; ")
}

getMode = function(x){
  x = x[which(!is.na(x))]
  val <- unique(x)
  return(val[which.max(tabulate(match(x, val)))])
}



################################################################################
for(i in 1:nrow(template)){
  row = template[i,]
  if(!file.exists(row$filePath)){
    warning(glue::glue("file {row$filePath} does not exist"))
    next
  }
  tic(row$datasetName)
  # load dataset
  dataset = rio::import(row$filePath)
  # convert column names to lower
  names(dataset) = tolower(names(dataset))
  # create new dataset-level variables
  datasetVars = data %>% 
    filter(datasetID == row$datasetID) %>% 
    select(varName, transform, Rtransform, Rfunction, summaryStatistic, value) %>% 
    mutate(transform = coalesce(Rtransform,transform,value)) %>%
    mutate(transform = case_when(str_detect(value,"concat")~str_replace_all(value,"concat","paste"),TRUE ~transform)) %>% 
    select(varName,transform) %>% 
    pivot_wider(names_from = "varName", values_from = "transform", values_fn = list) %>% 
    unchop(everything()) %>% 
    mutate_all(tolower)
  newVariables = names(datasetVars)
  newDataset = dataset
  names(dataset)
  newDataset = newDataset %>% 
    mutate_all(as.character)
  for(new in newVariables){
    newDataset[[new]] = tryCatch(eval(parse(text = datasetVars[[new]]), envir = newDataset),error = function(e){
      warning(paste("Error calculating variable",new,"for dataset",row$datasetName,"-- ID",row$datasetID))
      return(NA)
    })
  }
  
  # define rows to include
  row_categories = categories %>% 
    dplyr::filter(datasetID == row$datasetID)
  keyVar = row_categories %>% 
    dplyr::pull(variable) %>%
    unique() %>%
    tolower()
  val = row_categories %>% 
    dplyr::pull(value) %>%
    unique %>%
    tolower()
  if(isTRUE(any(keyVar == "function"))){
    if(isTRUE(any(val == "all"))){
      CMID = row_categories %>% 
        dplyr::filter(value == "all") %>%
        dplyr::pull(CMID) %>%
        unique()
      CMName = row_categories %>%
        dplyr::filter(value == "all") %>%
        dplyr::pull(CMName) %>%
        unique()
      newDataset[['CMName']] = CMName
      newDataset[['CMID']] = CMID
    } else {
      stop("Function is not all; other functions are not yet implemented")
    }
  } else {
    join_categories = row_categories %>% 
      mutate(CMID = coalesce(equivalentCMID,CMID),
             CMName = coalesce(equivalentCMName, CMName)) %>%
      select(CMName, CMID, variable,value) %>% 
      filter(variable != "variable") %>% 
      pivot_wider(names_from = "variable", values_from = "value") %>% 
      unchop(everything())
    if(nrow(join_categories) == 0){
      warning(glue::glue("No categories found for dataset {row$datasetName} -- ID {row$datasetID}"))
      next
    }
    newDataset = inner_join(newDataset, join_categories)
  }
  # subset dataset to only include variables in the stack
  newDataset = newDataset[,c("CMID","CMName",newVariables)]
  
  newDataset[['datasetID']] = row$datasetID
  newDataset[['datasetName']] = row$datasetName
  # save result
  rio::export(newDataset,glue::glue('temp/{row$datasetID}-{row$stackID}.csv'))
  toc()
}

##########################################################
stackids = template$stackID %>% unique()
for(s in stackids){
  stacks = template %>% dplyr::filter(stackID == s) %>% dplyr::pull(datasetID)
  stack = purrr::map_dfr(stacks,function(id){
    if(file.exists(paste0('temp/',id,'-',s,'.csv'))){
      rio::import(paste0('temp/',id,'-',s,'.csv')) %>%
        dplyr::mutate_all(as.character)
    }
  })
  
  # aggregate
  aggBy = data %>%
    dplyr::filter(stackID == s) %>%
    dplyr::pull(aggBy) %>%
    unique
  if(isTRUE(aggBy == 'category')){
    # take the mean of any numeric variable with no summaryStatistic and the mode of any character variable with no summaryStatistic
    # Determine which variables are numeric and which are character
    xVars = names(stack)
    type = NULL
    for(i in 1:length(xVars)){
      x = stack[[xVars[i]]][which(!is.na(stack[[xVars[i]]]))]
      xN = suppressWarnings(x %>% as.numeric() %>% .[which(!is.na(.))])
      if(length(x) == length(xN)) type[i] = "numeric" else type[i] = "character"
    }
    xVars = tibble::tibble(col = xVars, type = type)
    nVars = xVars %>% dplyr::filter(type == "numeric") %>% dplyr::pull(col)
    cVars = xVars %>% dplyr::filter(type == "character") %>% dplyr::pull(col)
    
    # group by gVars and take the mode of cVars and the median of nVars
    stack = stack %>% 
      group_by(CMID,CMName,datasetID) %>% 
      mutate_at(vars(any_of(nVars)),as.numeric) %>%
      summarize(
        dplyr::across(tidyselect::any_of(cVars), ~getMode(.x)),
        dplyr::across(tidyselect::any_of(nVars), ~median(.x,na.rm = T))
      )
  }
    
  rio::export(stack,paste0('temp/stack',s,'.csv'))
}

output = if(file.exists(glue::glue('temp/stack{stackids[1]}.csv'))) rio::import(glue::glue('temp/stack{stackids[1]}.csv'))
stack1 = stackids[1]
stackids = stackids[which(stackids != stackids[1])]
while(length(stackids) > 0){
  fn = paste0('stack',stackids[1])
  tmpOutput = if(file.exists(glue::glue('temp/{fn}.csv'))) rio::import(glue::glue('temp/{fn}.csv'))
  output = dplyr::full_join(output,tmpOutput, by = 'CMID', suffix = c(paste0('_',stack1),paste0('_',stacks[1])), na_matches = 'never', multiple = "all")
  stackids = stackids[which(stackids != stackids[1])]
}
rio::export(output,glue::glue('{template$mergingID[1]}-output.csv'))
# cleanup
unlink('temp',recursive = T)
toc()

