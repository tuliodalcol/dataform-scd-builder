function DATASET_PATH() {
    return dataform.projectConfig.defaultDatabase
        + "."
        + dataform.projectConfig.defaultSchema ;
}

module.exports = {
    DATASET_PATH
}