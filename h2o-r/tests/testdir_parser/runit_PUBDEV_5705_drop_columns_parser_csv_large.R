setwd(normalizePath(dirname(
  R.utils::commandArgs(asValues = TRUE)$"f"
)))
source("../../scripts/h2o-r-test-setup.R")

# Tests parsing of encrypted data
test.parseEncrypted <- function() {
  nrow <- 10000
  ncol <- 500
  seed <- 987654321
  frac1 <- 0.16
  f1 <-
    h2o.createFrame(
      rows = nrow,
      cols = ncol,
      randomize = TRUE,
      categorical_fraction = frac1,
      integer_fraction = frac1,
      binary_fraction = frac1,
      time_fraction = frac1,
      string_fraction = frac1,
      seed = seed,
      missing_fraction = 0.1
    )
  filePath <- getwd()
  fileName <- paste0(filePath, '/tempFrame.csv')
  
  h2o.downloadCSV(f1, fileName) # save generated file into csv
  skip_front <- c(1)
  skip_end <- c(h2o.ncol(f1))
  set.seed <- seed
  onePermute <- sample(h2o.ncol(f1))
  skipall <- onePermute
  skip10Per <- onePermute[1:floor(h2o.ncol(f1) * 0.1)]
  skip20Per <- onePermute[1:floor(h2o.ncol(f1) * 0.2)]
  skip99Per <- onePermute[1:floor(h2o.ncol(f1) * 0.99)]
  
  # test skipall for h2o.importFile
  e <-
    tryCatch(
      assertCorrectSkipColumns(fileName, skipall, TRUE),
      error = function(x)
        x
    )
  print(e)
  # test skipall for h2o.uploadFile
  e2 <-
    tryCatch(
      assertCorrectSkipColumns(fileName, skipall, FALSE),
      error = function(x)
        x
    )
  print(e2)
  
  # # skip 10% of the columns randomly
  # print("Testing skipping 10% of columns")
  # assertCorrectSkipColumns(fileName, skip10Per, TRUE) # test importFile
  # assertCorrectSkipColumns(fileName, skip10Per, FALSE) # test uploadFile
  # 
  # # skip 20% of the columns randomly
  # print("Testing skipping 20% of columns")
  # assertCorrectSkipColumns(fileName, skip20Per, TRUE) # test importFile
  # assertCorrectSkipColumns(fileName, skip20Per, FALSE) # test uploadFile
  
  # skip 90% of the columns randomly
  print("Testing skipping 99% of columns")
  assertCorrectSkipColumns(fileName, skip99Per, TRUE) # test importFile
  assertCorrectSkipColumns(fileName, skip99Per, FALSE) # test uploadFile
  
  if (file.exists(fileName))
    file.remove(fileName)
}

assertCorrectSkipColumns <-
  function(inputFileName,
           skip_columns,
           use_import) {
    if (use_import) {
      wholeFrame <<-
        h2o.importFile(inputFileName, skipped_columns = skip_columns)
    } else  {
      wholeFrame <<-
        h2o.uploadFile(inputFileName, skipped_columns = skip_columns)
    }
    
    f1R <- read.csv(inputFileName) # complete file read by R
    expect_true(h2o.nrow(wholeFrame)==nrow(f1R))
    cfullnames <- names(f1R)
    f2R <- as.data.frame(wholeFrame)
    cskipnames <- names(f2R)
    skipcount <- 1
    rowNum <- h2o.nrow(f1R)
    for (ind in c(1:length(cfullnames))) {
      if (cfullnames[ind] == cskipnames[skipcount]) {
        for (rind in c(1:rowNum)) {
          if (is.na(f1R[rind, ind]) || f1R[rind, ind]=="")
            expect_true(is.na(f2R[rind, skipcount]), info=paste0("expected NA but received: ", f2R[rind, skipcount], " in row: ", rind, " with column name: ", cfullnames[ind], " and skipped column name ", cskipnames[skipcount], sep=" "))
          else if (is.numeric(f1R[rind, ind])) {
            expect_true(abs(f1R[rind, ind]-f2R[rind, skipcount])<1e-10, info=paste0("expected: ", f1R[rind, ind], " but received: ", f2R[rind, skipcount], " in row: ", rind, " with column name: ", cfullnames[ind], " and skipped column name ", cskipnames[skipcount], sep=" "))
         } else
            expect_true(f1R[rind, ind] == f2R[rind, skipcount], info=paste0("expected: ", f1R[rind, ind], " but received: ", f2R[rind, skipcount], " in row: ", rind, " with column name: ", cfullnames[ind], " and skipped column name ", cskipnames[skipcount], sep=" "))
        }
        skipcount <- skipcount + 1
        if (skipcount > h2o.ncol(f2R))
          break
      }
    }
  }

doTest("Test Parse Encrypted data", test.parseEncrypted)