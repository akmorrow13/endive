# locally formats and saves submission correctly
# matches correct regions with scored regions
# truePath: like ladder_regions.blacklistfiltered.bed or test_regions.blacklistfiltered.bed
# predictionsPath: input tsv saved from TestPipeline.scala, like L.EGR1.K562.tab
# outputPath: final output, like L.EGR1.K562.tab (L for leaderboard, F for final)
def format_and_save_submission(truePath, predictionsPath, outputPath):
    infile = open(predictionsPath, "r")
    template = open(truePath, "r")
    outfile = open(outputPath, "w")

    dict = {}
    for line in infile:
        x = line.split("\t")
        dict[x[0] + "\t" + x[1] + "\t" + x[2]] = x[3].split("\n")[0]

    found_count = 0
    not_found_count = 0

    for line in template:
        x = line.split("\n")
        try:
            outfile.write(x[0] + "\t" + dict[x[0]] + "\n")
            found_count += 1
        except:
            print("error, cant find in true file", x)
            outfile.write(x[0] + "\t" + "0.0\n")
            not_found_count += 1
