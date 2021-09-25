str_escape_tex <- function(.str){
  stringr::str_replace_all(.str, "[_%$]", "\\\\\\0")
}

plot_to_file <- function(.plot, .path, .width_rel=1, .height_rel=1){
  
  .width_page <- 210-2*25 # 210mm Din A4 und 2
  
  .luatex_preamble <- 
    c("\\usepackage{tikz}\n",
      "\\IfFileExists{luatex85.sty}{\\usepackage{luatex85}}{}\n",
      "\\usepackage[active,tightpage,psfixbb]{preview}\n",
      "\\usepackage{microtype}\n",
      "\\usepackage{fontspec}\n",                           
      "\\usepackage{unicode-math}\n",
      "\\setmainfont{FranziskaPro}\n",
      "\\setmonofont[Scale=MatchLowercase]{LetterGothicMono}\n",
      "\\setmathfont[Scale=MatchLowercase]{Tex Gyre Pagella Math}\n",
      "\\setmathfont[range=up]{Franziska Pro}\n",
      "\\setmathfont[range=it]{Franziska Pro Italic}\n",
      "\\setmathfont[range=bfup]{Franziska Pro Bold}\n",
      "\\setmathfont[range=bfit]{Franziska Pro BoldItalic}\n",
      "\\setmathfont[Scale=MatchLowercase,range=up/{greek,Greek}]{Alegreya}\n",
      "\\setmathfont[Scale=MatchLowercase,range=it/{greek,Greek}]{Alegreya Italic}\n",
      "\\setmathfont[Scale=MatchLowercase,range=bfup/{greek,Greek}]{Alegreya Bold}\n",
      "\\setmathfont[Scale=MatchLowercase,range=bfit/{greek,Greek}]{Alegreya BoldItalic}\n",
      "\\setmathfont[Scale=MatchLowercase,range={cal,bfcal}]{XITS Math}\n",
      "\\PreviewEnvironment{pgfpicture}\n",
      "\\setlength\\PreviewBorder{0pt}\n")
  
  .path_ext <- fs::path_ext(.path)
  .path_abs <- fs::path_abs(.path)
  
  if(!.path_ext %in% c("pdf", "png")){stop("invalid file-extension")}
  
  .path_abs_pdf <- fs::path_ext_set(.path_abs, "pdf")
  .path_abs_png <- fs::path_ext_set(.path_abs, "png")
  
  .path_tmp_dir <- fs::dir_create(fs::file_temp("dir"))
  .path_tmp_tex <- fs::path_abs("tikz_graphic.tex", start=.path_tmp_dir)
  
  ggsave(filename=.path_tmp_tex, plot=.plot, device=tikzDevice::tikz, 
         width=.width_page*.width_rel, height=.width_page*.height_rel, 
         units="mm", engine="luatex", packages=.luatex_preamble, 
         standAlone=TRUE)

  .cmd_tex_to_pdf <- 
    glue::glue("cd {fs::path_dir(.path_tmp_tex)} && lualatex --shell-escape ",
               "--interaction=batchmode {fs::path_file(.path_tmp_tex)}")
  
  system(.cmd_tex_to_pdf)
  
  fs::file_copy(fs::path_ext_set(.path_tmp_tex, "pdf"), .path_abs_pdf,
                overwrite=TRUE)
  
  fs::dir_delete(.path_tmp_dir)
  
  .cmd_pdf_to_png <- 
    glue::glue("cd {fs::path_dir(.path_abs_png)} && pdftoppm -png ", 
               "-rx 300 -ry 300 -singlefile {.path_abs_pdf} ", 
               "{fs::path_file(fs::path_ext_remove(.path_abs_png))}")
  
  system(.cmd_pdf_to_png)
  
}
