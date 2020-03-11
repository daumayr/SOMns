# This Makefile relies on LaTeX-Mk
# See: http://latex-mk.sourceforge.net/

NAME=paper

DATA_FILES=$(wildcard evaluation/*.bz)
KNITR=$(wildcard evaluation/*.Rnw)
KNITR_TEX=$(KNITR:.Rnw=.tex)
KNITR_TEX=$(patsubst evaluation/%.Rnw,gen/%.tex,$(KNITR))
TEXSRCS=$(NAME).tex $(wildcard evaluation/*.tex) #$(KNITR_TEX)
USE_PDFLATEX=true
BIBTEXSRCS=$(NAME).bib

CLEAN_FILES+=$(wildcard *.synctex.gz) $(wildcard *.fdb_latexmk) $(wildcard *.fls) $(KNITR_TEX) comment.cut

gen/performance.tex: ../evaluation/performance.Rnw gen ../evaluation/scripts/*.R $(DATA_FILES)
	../evaluation/scripts/knit.R $< $@

# We are going to try a couple of standard locations to find LaTeX-Mk:
-include ~/.local/share/latex-mk/latex.gmk
-include /usr/local/share/latex-mk/latex.gmk
-include /opt/local/share/latex-mk/latex.gmk
-include /usr/share/latex-mk/latex.gmk

gen:
	-mkdir gen

open:
	mate $(TEXSRCS)

clobber: clean
	rm -Rf gen cache

html5:
	-mkdir tmp-html
	~/Projects/misc/latex-to-html5/ht-latex $(NAME).tex tmp-html/

zip:
	zip paper.zip $(TEXSRCS) $(BIBTEXSRCS) scripts/collab.sty ACM-Reference-Format.bst acmart.cls $(NAME).bbl figures/process-view.pdf figures/protocol-class-diagram.pdf figures/protocol-overview.pdf figures/stm-example.pdf figures/system-view.pdf

latexdiff:
	-mkdir submission-latex
	git archive --format=tar --prefix=submission-latex/ dls-2017 | tar xf -
	latexdiff submission-latex/$(NAME).tex $(NAME).tex > diff.tex
	pdflatex diff
	pdflatex diff
	bibtex diff
	pdflatex diff
	pdflatex diff

lacheck:
	lacheck $(NAME).tex

chktex:
	chktex $(NAME).tex

lint: chktex lacheck

arxiv: all
	mkdir -p arxiv/scripts
	mkdir -p arxiv/images
	# references.bib: removed, because it confuses arxiv
	cp paper.bbl acmart.cls ACM-Reference-Format.bst arxiv/
	cp scripts/collab.sty arxiv/scripts/
	cp images/*.pdf arxiv/images/
	latexpand --empty-comments paper.tex --output arxiv/paper.tex
	zip --junk-paths arxiv.zip arxiv/*
