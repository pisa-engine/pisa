.PHONY: all clean

all: pipeline

pipeline: pipeline.tex
	pdflatex pipeline
	convert -density 100 pipeline.pdf ../_static/img/pipeline.png

clean:
	rm -f pipeline.aux pipeline.log pipeline.pdf
