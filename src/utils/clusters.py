# CLUSTERS = [
#     # "common-dstore-ok-archiving", # пустой
#     # "common-egrul-egrip-client", # пустой
#     # "common-filestore-techsupp", # ок
#     # "common-gisnr-request-service", # пустой
#     # "common-knowledge-base", # ок
#     # "common-lko", # ок
#     # "common-planner", # ок
#     # "common-smev-adapter", # ок
#     # "common-tech-supp",  # ок
#     # "common-user-control",  # ок
#     # "epz-activity", # пустой
#     # "epz-analytics-aggregator", # пустой
#     # "epz-complaint", # ок
#     # "epz-contract-fz223", # ок
#     # "epz-contract-opensearch", # пустой в application_stats_error_total
#     # "epz-contract-reporting",  # пустой в application_stats_error_total
#     # "epz-contract", # ок
#     # "epz-control-result", # ок
#     # "epz-customer-reports", # ок
#     # "epz-customer223", # ок
#     # "epz-dishonest-supplier", # ок
#     # "epz-dizk", # ок
#     # "epz-etp", # ок
#     # "epz-farm", # пустой в application_stats_error_total
#     # "epz-goods-works-services", # ок
#     # "epz-inspection-plan", # ок
#     # "epz-ktru-opensearch", # пустой application_stats_error_total
#     # "epz-ktru", # ок
#     # "epz-legal-acts", # ок
#     # "epz-main", # ок
#     # "epz-mechproducts", # этот кластер пустой
#     # "epz-mobile-push", # ок
#     # "epz-mobile-web", # ок
#     # "epz-normalization-rules", # ок
#     # "epz-nsi", # ок
#     # "epz-oboz", # ок
#     # "epz-opendata", # ок
#     # "epz-order-clause", # ок
#     # "epz-order-doc", # ок
#     # "epz-order-opensearch", # пустой application_stats_error_total
#     # "epz-order-plan", # ок
#     "epz-order",
#     # "epz-organization", # ок
#     # "epz-pricereq", # ок
#     # "epz-purchase-plan-fz44", # ок
#     # "epz-rdik-opensearch", # пустой в application_stats_error_total
#     # "epz-rdik", # ок
#     # "epz-rep", # ок
#     # "epz-revenue", # пустой в application_stats_error_total
#     # "epz-rirz", # пустой в application_stats_error_total
#     # "epz-rkpo", # ок
#     # "epz-ropt", # пустой в application_stats_error_total
#     # "epz-sphinx-server-extractor-223", # ок
#     # "epz-sphinx-server-extractor-44-cr", # ок
#     # "epz-sphinx-server-extractor-44-doc", # ок
#     # "epz-sphinx-server-extractor-44-epz", # ок
#     # "epz-sphinx-server-extractor-44-gza", # ок
#     # "epz-sphinx-server-extractor-44-op", # ок
#     # "epz-sphinx-server-extractor-44-rk44", # ок
#     # "epz-sphinx-server-extractor-44-rz44", # ок
#     # "epz-sphinx-server-extractor-94", # ок
#     # "epz-typal-clause", # ок
#     "epz-unscheduled-inspection",
#     # "fz223-clause", # ок
#     # "fz223-contract-account", # ок
#     # "fz223-contractprivate", # ок
#     # "fz223-customer-list", # пустой в application_stats_error_total
#     # "fz223-dstore-rc223-archiving", # пустой в application_stats_error_total
#     # "fz223-dstore-rc223", # ок
#     # "fz223-dstore-rn223-archiving", # пустой в application_stats_error_total
#     # "fz223-dstore-rn223", # ок
#     # "fz223-dstore-rp223-archiving", # пустой в application_stats_error_total
#     # "fz223-dstore-rp223", # ок
#     # "fz223-exclusion-notice", # пустой в application_stats_error_total
#     # "fz223-filestore", # ок
#     # "fz223-fstore", # ок
#     # "fz223-guarantee", # пустой в application_stats_error_total
#     # "fz223-integration-export", # пустой в application_stats_error_total
#     # "fz223-list-gws", # ок
#     # "fz223-mech-engineering", # пустой в application_stats_error_total
#     # "fz223-monitoring", # пустой в application_stats_error_total
#     # "fz223-plan", # ок
#     # "fz223-ppa", # ок
#     # "fz223-private", # ок
#     # "fz223-purchase-account", # ок
#     # "fz223-purchase-monitoring", # пустой в application_stats_error_total
#     # "fz223-purchase", # ок
#     # "fz223-urd-eis", # пустой в application_stats_error_total
#     # "lkp-auth", # ок
#     # "lkp-bus", # пустой в application_stats_error_total
#     "lkp-complaints",
#     # "lkp-dstore-ea-archiving", # пустой в application_stats_error_total
#     # "lkp-dstore-ea", # ок
#     # "lkp-dstore-lkp-archiving", # пустой в application_stats_error_total
#     # "lkp-dstore-lkp", # пустой в application_stats_error_total
#     "lkp-eact",
#     "lkp-edo-adapter-lkp",
#     "lkp-els",
#     "lkp-entrypoint",
#     "lkp-eruz",
#     "lkp-filestore-eruz",
#     "lkp-filestore-main",
#     "lkp-fstore-main",
#     "lkp-mail",
#     "lkp-master-data",
#     "lkp-nsi",
#     "lkp-pmd",
#     "lkp-ppa",
#     "lkp-purchase-subscriptions",
#     "lkp-pzk",
#     "lkp-rbg",
#     "lkp-rsao",
#     "lkp-rsd",
#     "lkp-self-employed-adapter",
#     "lkp-sphinx-search",
#     "lkp-urd-eis",
#     "priv-agreements",
#     "priv-audit",
#     "priv-auth",
#     "priv-btk",
#     "priv-budget-control",
#     "priv-bus",
#     "priv-calendar",
#     "priv-controls",
#     "priv-customer-report",
#     "priv-dizk",
#     "priv-do",
#     "priv-documents-extractor",
#     "priv-dstore-fcs-archiving",
#     "priv-dstore-fcs",
#     "priv-dstore-rk-archiving",
#     "priv-dstore-rk",
#     "priv-dstorebus",
#     "priv-ebbus",
#     "priv-edo-adapter-rk",
#     "priv-entrypoint",
#     "priv-excel-export",
#     "priv-filestore-eis",
#     "priv-filestore-rdik",
#     "priv-filestore-rkshard",
#     "priv-filestore-uz",
#     "priv-fstore-eis",
#     "priv-fstore-rkshard",
#     "priv-fstore-uz",
#     "priv-int-ppa",
#     "priv-integration-monitoring",
#     "priv-ktru",
#     "priv-master-data",
#     "priv-npa",
#     "priv-nrbus",
#     "priv-nsi-facade",
#     "priv-nsieis",
#     "priv-pbo",
#     "priv-ppa-bodorskp",
#     "priv-ppa-rdik",
#     "priv-ppa-rgk",
#     "priv-ppa",
#     "priv-pricereq",
#     "priv-priz",
#     "priv-prz",
#     "priv-psk",
#     "priv-rbg",
#     "priv-rdik-ext-async-runner",
#     "priv-rdik",
#     "priv-rec",
#     "priv-rep",
#     "priv-rgk",
#     "priv-rirz",
#     "priv-ris",
#     "priv-rkpo",
#     "priv-roko",
#     "priv-ropt",
#     "priv-routes",
#     "priv-rpg",
#     "priv-rpgz",
#     "priv-rpk",
#     "priv-rpnz",
#     "priv-rppu",
#     "priv-rpz",
#     "priv-rskp",
#     "priv-rsktru",
#     "priv-rsok",
#     "priv-sbo",
#     "priv-spell-checker",
#     "priv-supplier",
#     "priv-urd-eis",
#     "priv-widgets",
# ]
CLUSTERS = [
    "common-filestore-techsupp",  # ок
]
