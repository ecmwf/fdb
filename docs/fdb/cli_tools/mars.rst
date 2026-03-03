MARS request
============

A **MARS request** is the way to specify an action on a set of fields or observations. The directives specified in a MARS request have the following syntax:
::

   verb,
      keyword1 = value 1,
      ...      = ...,
      keywordN = value N 

Where:

:verb: identifies the action to be taken, e.g. retrieve or list.
:keyword: is a known MARS variable, e.g. type or date
:value: is the value assigned to the keyword, e.g. Analysis or temperature.
  
| Keywords may be assigned a **single value**, a **list of values** or a **range of values**.  
| A **list** is indicated by the separator **/**  
| A **range** is indicated by using the keywords **to** as well as **/** and **by**.  
| Examples of different formats for values are given in the table below.

===============  ===============
Format	         Example
===============  ===============
single value     ``param = temperature/SSRD``
list of values   ``step = 12/24/48``
range of values  ``date = 19990104/to/19990110/by/2``
===============  ===============

The following example shows a retrieve request with a mix of single, list and range of values:
::

  retrieve,  
     class    = od,  
     stream   = oper,  
     expver   = 1,  
     date     = -1,  
     type     = analysis,  
     levtype  = model levels,  
     levelist = 1/to/91,  
     param    = temperature,  
     grid     = 0.1/0.1

Retrieve requests have to specify, at least, directives to identify the data. The ones to identify fields are defined in the MARS Catalogue. Data manipulation (post-processing, such as **grid**) directives are optional, depending on user needs.
