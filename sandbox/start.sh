julia -t auto --project=. -e "using Revise; using DotEnv; using Pkg; Pkg.develop(path=\"..\"); using ADRIAReefGuideWorker; DotEnv.load!();" -i
