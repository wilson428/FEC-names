This Python 2.7 code loads Federal Elections Commission data, in csv format, into a SQLite database.
Once this is accomplished, it converts the raw strings of names into first and last names, then reduces
those records to unique first and last names, retaining the frequency of those names and the splits
in donations between candidates.

This is not the cleanest data in the world, so a small number of names will be incorrectly loaded. 

To reduce double counting of names, middle initials are ignored. To prevent undercounting of
common names, names are assumed to be unique by zip code and candidate to which the donation was made.
This is necessary because many individuals are listed many times if they gave in installments.

Thus, two distinct John Smiths in area code 22901 both giving to Mitt Romney will be counted as one person. 
The collision space here appears to be zero or near zero. 

The source code is heavily commented with more detaisl. This is licensed under BSD 2-Clause License (see LICENSE.txt)
http://opensource.org/licenses/BSD-2-Clause

All comments, questions or concerns are welcome! 
Chris Wilson
cewilson@yahoo-inc.com
https://github.com/wilson428

PERMANENT DISCLOSURE
I am a reporter by training and a largely self-taught programmer, so I can guarantee that the code here 
is not as elegant or Pythonic as it ought to be. I'm posting this in the interest of transparency for
anyone who wants to check the methodology or see how this sort of project is accomplished. I would only
advise against assuming it's the BEST way for it to be accomplished. If you see places to improve, please
let me know!