julia -t auto --project=. -e "using Pkg; Pkg.instantiate(); Pkg.add(\"DotEnv\"); Pkg.add(\"Revise\"); Pkg.develop(path=\"..\"); using ADRIAReefGuideWorker; Pkg.precompile();"
