# R syntax
library(rio)
library(dplyr)
library(tidyselect)
library(tidyr)
library(purrr)
library(tictoc)
tic("Merge")
# set working directory
wd = 'E:/Dropbox\ (ASU)/RA/CatMapper/Code/SocioMap/data/surveys'
setwd(wd)
if(!dir.exists('temp'))
  dir.create('temp')
# load categories used in merge
template = rio::import('template.csv') %>% dplyr::mutate_all(as.character)
categories = rio::import('mergingCategories.csv') %>% dplyr::mutate_all(as.character) %>%
  dplyr::mutate_all(as.character) %>% tidyr::separate_rows(datasetID, sep=" \\|\\| ") %>%
  dplyr::mutate_all(stringr::str_trim)
metadata = rio::import('metadata.csv') %>% dplyr::mutate_all(as.character)
variables = rio::import('variables.csv')
stackVariables = tryCatch(rio::import('stackVariables.csv'),error = function(e)return(tibble::tibble()))

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
  row = template[i,] %>% dplyr::mutate_all(as.character)
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
  if("datasetID" %in% names(variables)){
    datasetVars = variables  %>%
      dplyr::filter(datasetID == row$datasetID) %>%
      dplyr::select(varName,transform) %>%
      dplyr::distinct_all() %>%
      dplyr::mutate_at(dplyr::vars(transform),stringr::str_replace_all,'""','"') %>%
      dplyr::mutate_at(dplyr::vars(transform),stringr::str_replace_all,'""','"') %>%
      tidyr::pivot_wider(names_from = 'varName',values_from = 'transform')
    newVariables = names(datasetVars)
    newDataset = dataset
    for(new in newVariables){
      newDataset[[new]] = tryCatch(eval(parse(text = datasetVars[[new]]), envir = newDataset),error = function(e){
        warning(paste("Error calculating variable",new,"for dataset",row$datasetName,"-- ID",row$datasetID))
        return(NA)
      })
    }
  }
  # define variables to include
  keyVar = categories %>%
    dplyr::filter(datasetID == row$datasetID) %>%
    dplyr::pull(variable) %>%
    unique %>%
    tolower()
  if(isTRUE(keyVar == "function")){
    val = categories %>%
      dplyr::filter(datasetID == row$datasetID) %>%
      dplyr::pull(value) %>%
      unique %>%
      tolower()
    if(val == "all"){
      continue = TRUE
      tmpCategories = categories %>%
        dplyr::filter(datasetID == row$datasetID) %>%
        dplyr::select(-rowid,-variable,-value)
      keyVar = "CMName"
    }
  } else continue = FALSE
  if(isFALSE(continue)){
    if(!any(keyVar %in% names(dataset))) {
      warning(paste("the key variable does not exist in dataset",row$datasetName,"-- ID",row$datasetID,": please correct in the database"))
      toc()
      next
    }
    keyVarClass = dataset[[keyVar[1]]] %>% class

    if(nrow(stackVariables) > 0){
      vars = stackVariables %>%
        dplyr::filter(stackID == row$stackID) %>%
        dplyr::pull(Key) %>%
        c(keyVar,newVariables)
    } else {
      vars = c(keyVar,newVariables)
    }
    # create data frame with variables and IDs to join on
    tmpCategories = categories %>%
      dplyr::filter(datasetID == row$datasetID) %>%
      tidyr::pivot_wider(names_from = variable, values_from = value, values_fn = list) %>%
      tidyr::unchop(tidyselect::any_of(keyVar),keep_empty = T) %>%
      dplyr::mutate_at(dplyr::vars(tidyselect::all_of(keyVar)),list(~eval(parse(text = glue::glue('tryCatch(as.{keyVarClass}(.x),error = function(e)return(.x))')))))
    # join categories to dataset
    if(length(keyVar) == 1){
      newDataset = newDataset %>%
        dplyr::select(tidyselect::any_of(vars)) %>%
        dplyr::inner_join(tmpCategories, by = keyVar) %>%
        dplyr::mutate(datasetID = row$datasetID)
    } else {
      newDataset = purrr::map(keyVar,function(key){
        keylist = keyVar[which(key != keyVar)]
        newDataset %>%
          dplyr::select(tidyselect::any_of(vars)) %>%
          dplyr::inner_join(tmpCategories %>% dplyr::filter(!is.na(!!as.name(key))), by = key,multiple = "all") %>%
          dplyr::filter(!is.na(!!as.name(key))) %>%
          slice(which(is.na(!!as.name(paste0(keylist,'.y'))) | !!as.name(paste0(keylist,'.x')) == !!as.name(paste0(keylist,'.y'))))
      })
      newDataset = do.call(dplyr::full_join,newDataset)
    }
  } else {
    vars = ""
    newDataset = newDataset %>% dplyr::mutate(datasetID = row$datasetID) %>%
      dplyr::left_join(tmpCategories, by = "datasetID")
  }
  if(nrow(newDataset) < 1){
    warning(paste("No rows in dataset",row$datasetName))
    toc()
    next
  }
  # join metadata
  newDataset = dplyr::left_join(newDataset,metadata, by = 'datasetID')
  # select only required variables
  newDataset = newDataset %>%
    dplyr::select(datasetID,tidyselect::any_of(c("year","District")),CMID,CMName,tidyr::any_of(c(vars,newVariables))) %>%
    tidyr::unite("Key",tidyr::any_of(keyVar), sep = "; ", na.rm = T)
  # save result
  rio::export(newDataset,glue::glue('temp/{row$datasetID}-{row$stackID}.csv'))
  toc()
}
################################################################################
stackids = template$stackID %>% unique
for(s in stackids){
  stacks = template %>% dplyr::filter(stackID == s) %>% dplyr::pull(datasetID)
  stack = purrr::map_dfr(stacks,function(id){
    if(file.exists(paste0('temp/',id,'-',s,'.csv'))){
      rio::import(paste0('temp/',id,'-',s,'.csv')) %>%
        dplyr::mutate_all(as.character)
    }
  })
  stackVars = variables %>%
    dplyr::filter(stackID == s) %>%
    dplyr::select(varName,transform) %>%
    dplyr::distinct_all() %>%
    dplyr::mutate_at(dplyr::vars(transform),stringr::str_replace_all,'""','"') %>%
    dplyr::mutate_at(dplyr::vars(transform),stringr::str_replace_all,'""','"') %>%
    tidyr::pivot_wider(names_from = 'varName',values_from = 'transform')
  newVariables = names(stackVars)
  for(new in newVariables){
    stack[[new]] = tryCatch(eval(parse(text = stackVars[[new]]), envir = stack),error = function(e){
      print(paste("Error calculating variable",new,"for stack",s))
      return(NA)
    })
  }
  # aggregate
  aggBy = variables %>%
    dplyr::filter(stackID == s) %>%
    dplyr::pull(aggBy) %>%
    unique
  if(isTRUE(aggBy == 'category')){
    # take the mean of any numeric variable with no summaryStatistic and the mode of any character variable with no summaryStatistic
    # Determine which variables are numeric and which are character
    xVars = stackVariables %>%
      dplyr::filter(stackID == s) %>%
      dplyr::pull(Key) %>%
      unique() # remove extra variables
    type = NULL
    for(i in 1:length(xVars)){
      x = stack[[xVars[i]]][which(!is.na(stack[[xVars[i]]]))]
      xN = suppressWarnings(x %>% as.numeric() %>% .[which(!is.na(.))])
      if(length(x) == length(xN)) type[i] = "numeric" else type[i] = "character"
    }
    xVars = tibble::tibble(col = xVars, type = type)
    nVars = xVars %>% dplyr::filter(type == "numeric") %>% dplyr::pull(col)
    cVars = xVars %>% dplyr::filter(type == "character") %>% dplyr::pull(col)

    sumVarsDF = variables %>%
      dplyr::filter(!is.na(summaryStatistic) & summaryStatistic != "") %>%
      dplyr::group_by(summaryStatistic) %>%
      dplyr::group_split()
    sumFunText = purrr::map_chr(sumVarsDF,function(df){
      fn = switch (df$summaryStatistic %>% unique,
                   mean = "~mean(.x,na.rm = T)",
                   median = "~median(.x,na.rm = T)"
      )
      glue::glue("
                       dplyr::across(tidyselect::any_of(c('{paste(df$varName,collapse = \"','\")}')), {fn}),
                 ")
    })
    sumResult = glue::glue(
      'dplyr::summarize(stack,dplyr::across("Key",CM2list),',
      sumFunText,
      'dplyr::across(tidyselect::any_of(nVars), ~median(.x,na.rm = T)),',
      'dplyr::across(tidyselect::any_of(cVars), ~getMode(.x,na.rm = T))',
      ')'
    )

    stack = stack %>%
      dplyr::group_by(dplyr::across(-tidyselect::any_of(unique(c(do.call(rbind,sumVarsDF)$varName,nVars,cVars,"Key"))))) %>%
      dplyr::mutate_at(dplyr::vars(nVars),as.numeric)
    stack = tryCatch(eval(parse(text = sumResult)),error = function(e){
      print(paste("Error aggregating variables for stack",s))
      return(NA)
    })
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

