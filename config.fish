fish_vi_key_bindings
set -gx PATH  /home/eecs/vaishaal/anaconda2/bin $PATH
set -gx EDITOR vim

set -gx LD_LIBRARY_PATH $LD_LIBRARY_PATH

function fish_prompt
  env FISH_VERSION=$FISH_VERSION PROMPTLINE_LAST_EXIT_CODE=$status bash ~/.config/fish/.shell_prompt.sh left
end

function fish_right_prompt
  env FISH_VERSION=$FISH_VERSION PROMPTLINE_LAST_EXIT_CODE=$status bash ~/.config/fish/.shell_prompt.sh right
end

set -gx JUPYTER_CONFIG_DIR /home/eecs/vaishaal/.jupyter
set -gx IPYTHONDIR /home/eecs/vaishaal/.ipython
set -gx JUPYTER_PATH /home/eecs/vaishaal/anaconda2/share/jupyter:/usr/local/share/jupyter:/usr/share/jupyter
set -gx SPARK_NUM_EXECUTORS 12
set -gx SPARK_EXECUTOR_CORES 16
set -gx KEYSTONE_MEM 50g

set -gx AWS_ACCESS_KEY_ID AKIAJ5XDCFWZOHFC4ESA
set -gx AWS_SECRET_ACCESS_KEY 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

function edit-command
    set -q EDITOR; or return 1
    set -l tmpfile (mktemp); or return 1
    commandline > $tmpfile
    eval $EDITOR $tmpfile
    commandline -r (cat $tmpfile)
    rm $tmpfile
end

bind \cx edit-command
