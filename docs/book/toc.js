// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded affix "><a href="introduction.html">Introduction</a></li><li class="chapter-item expanded affix "><li class="part-title">User Guide</li><li class="chapter-item expanded "><a href="guide/requirements.html"><strong aria-hidden="true">1.</strong> Requirements</a></li><li class="chapter-item expanded "><a href="guide/installation.html"><strong aria-hidden="true">2.</strong> Installation</a></li><li class="chapter-item expanded "><a href="guide/indexing-pipeline.html"><strong aria-hidden="true">3.</strong> Indexing Pipeline</a></li><li class="chapter-item expanded "><a href="guide/parsing.html"><strong aria-hidden="true">4.</strong> Parsing</a></li><li class="chapter-item expanded "><a href="guide/inverting.html"><strong aria-hidden="true">5.</strong> Inverting</a></li><li class="chapter-item expanded "><a href="guide/compressing.html"><strong aria-hidden="true">6.</strong> Compressing</a></li><li class="chapter-item expanded "><a href="guide/wand_data.html"><strong aria-hidden="true">7.</strong> &quot;WAND&quot; Data</a></li><li class="chapter-item expanded "><a href="guide/querying.html"><strong aria-hidden="true">8.</strong> Querying</a></li><li class="chapter-item expanded "><a href="guide/algorithms.html"><strong aria-hidden="true">9.</strong> Retrieval Algorithms</a></li><li class="chapter-item expanded "><a href="guide/reordering.html"><strong aria-hidden="true">10.</strong> Document Reordering</a></li><li class="chapter-item expanded "><a href="guide/sharding.html"><strong aria-hidden="true">11.</strong> Sharding</a></li><li class="chapter-item expanded "><a href="guide/threshold-estimation.html"><strong aria-hidden="true">12.</strong> Threshold Estimation</a></li><li class="chapter-item expanded affix "><li class="part-title">Tutorials</li><li class="chapter-item expanded "><a href="tutorial/robust04.html"><strong aria-hidden="true">13.</strong> Regression test for Robust04</a></li><li class="chapter-item expanded affix "><li class="part-title">CLI Reference</li><li class="chapter-item expanded "><a href="cli/compress_inverted_index.html"><strong aria-hidden="true">14.</strong> compress_inverted_index</a></li><li class="chapter-item expanded "><a href="cli/compute_intersection.html"><strong aria-hidden="true">15.</strong> compute_intersection</a></li><li class="chapter-item expanded "><a href="cli/count-postings.html"><strong aria-hidden="true">16.</strong> count-postings</a></li><li class="chapter-item expanded "><a href="cli/create_wand_data.html"><strong aria-hidden="true">17.</strong> create_wand_data</a></li><li class="chapter-item expanded "><a href="cli/evaluate_queries.html"><strong aria-hidden="true">18.</strong> evaluate_queries</a></li><li class="chapter-item expanded "><a href="cli/extract-maxscores.html"><strong aria-hidden="true">19.</strong> extract-maxscores</a></li><li class="chapter-item expanded "><a href="cli/extract_topics.html"><strong aria-hidden="true">20.</strong> extract_topics</a></li><li class="chapter-item expanded "><a href="cli/invert.html"><strong aria-hidden="true">21.</strong> invert</a></li><li class="chapter-item expanded "><a href="cli/kth_threshold.html"><strong aria-hidden="true">22.</strong> kth_threshold</a></li><li class="chapter-item expanded "><a href="cli/lexicon.html"><strong aria-hidden="true">23.</strong> lexicon</a></li><li class="chapter-item expanded "><a href="cli/map_queries.html"><strong aria-hidden="true">24.</strong> map_queries</a></li><li class="chapter-item expanded "><a href="cli/parse_collection.html"><strong aria-hidden="true">25.</strong> parse_collection</a></li><li class="chapter-item expanded "><a href="cli/partition_fwd_index.html"><strong aria-hidden="true">26.</strong> partition_fwd_index</a></li><li class="chapter-item expanded "><a href="cli/queries.html"><strong aria-hidden="true">27.</strong> queries</a></li><li class="chapter-item expanded "><a href="cli/read_collection.html"><strong aria-hidden="true">28.</strong> read_collection</a></li><li class="chapter-item expanded "><a href="cli/reorder-docids.html"><strong aria-hidden="true">29.</strong> reorder-docids</a></li><li class="chapter-item expanded "><a href="cli/sample_inverted_index.html"><strong aria-hidden="true">30.</strong> sample_inverted_index</a></li><li class="chapter-item expanded "><a href="cli/selective_queries.html"><strong aria-hidden="true">31.</strong> selective_queries</a></li><li class="chapter-item expanded "><a href="cli/shards.html"><strong aria-hidden="true">32.</strong> shards</a></li><li class="chapter-item expanded "><a href="cli/stem_queries.html"><strong aria-hidden="true">33.</strong> stem_queries</a></li><li class="chapter-item expanded "><a href="cli/taily-stats.html"><strong aria-hidden="true">34.</strong> taily-stats</a></li><li class="chapter-item expanded "><a href="cli/taily-thresholds.html"><strong aria-hidden="true">35.</strong> taily-thresholds</a></li><li class="chapter-item expanded "><a href="cli/thresholds.html"><strong aria-hidden="true">36.</strong> thresholds</a></li><li class="chapter-item expanded affix "><li class="part-title">Specifications</li><li class="chapter-item expanded "><a href="specs/lookup-table.html"><strong aria-hidden="true">37.</strong> Lookup Table</a></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0].split("?")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
