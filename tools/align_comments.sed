# Indents '#' style comments following instructions in scripts to align on columns 17 and 29 (mimicking tab stops of 8)
#
# Example:
#  write 65 # Size
#  write 0x01s # Fetch
# is transformed to
#  write 65        # Size
#  write 0x01s     # Fetch
#
# Usage
# =====
# ufind src/main/scripts/org/reaktivity/specification/kafka/fetch.v5/ -name "*.rpt"| xargs -L 1 sed -i "~" -f tools/align_comments.sed
#

/^[^#]\{4\}[^#]* * #/s/  *#/ #/

s/^\([^#]\{3\} \)#/\1            #/
s/^\([^#]\{4\} \)#/\1           #/
s/^\([^#]\{5\} \)#/\1          #/
s/^\([^#]\{6\} \)#/\1         #/
s/^\([^#]\{7\} \)#/\1        #/
s/^\([^#]\{8\} \)#/\1       #/
s/^\([^#]\{9\} \)#/\1      #/
s/^\([^#]\{10\} \)#/\1     #/
s/^\([^#]\{11\} \)#/\1    #/
s/^\([^#]\{12\} \)#/\1   #/
s/^\([^#]\{13\} \)#/\1  #/
s/^\([^#]\{14\} \)#/\1 #/

s/^\([^#]\{16\} \)#/\1           #/
s/^\([^#]\{17\} \)#/\1          #/
s/^\([^#]\{18\} \)#/\1         #/
s/^\([^#]\{19\} \)#/\1        #/
s/^\([^#]\{20\} \)#/\1       #/
s/^\([^#]\{21\} \)#/\1      #/
s/^\([^#]\{22\} \)#/\1     #/
s/^\([^#]\{23\} \)#/\1    #/
s/^\([^#]\{24\} \)#/\1   #/
s/^\([^#]\{25\} \)#/\1  #/
s/^\([^#]\{26\} \)#/\1 #/


