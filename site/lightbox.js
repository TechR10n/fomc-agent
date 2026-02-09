// Lightweight, dependency-free lightbox for SVG diagrams.
// Attaches to diagrams that are not already inside an <a> (to avoid hijacking nav cards).
(function initDiagramLightbox() {
  function run() {
    const selector = 'img.diagram-img, img[src^="diagrams/"][src$=".svg"]';
    const candidates = Array.from(document.querySelectorAll(selector));
    const targets = candidates.filter((img) => {
      if (!(img instanceof HTMLImageElement)) return false;
      // If the image is inside a link, clicking should navigate (e.g., index cards).
      if (img.closest("a")) return false;
      return true;
    });

    if (!targets.length) return;

    let backdrop;
    let titleEl;
    let zoomImg;
    let openLink;
    let closeBtn;
    let lastActive;

    function ensureOverlay() {
      if (backdrop) return;

      backdrop = document.createElement("div");
      backdrop.className = "lightbox-backdrop";
      backdrop.hidden = true;

      const dialog = document.createElement("div");
      dialog.className = "lightbox-dialog";
      dialog.setAttribute("role", "dialog");
      dialog.setAttribute("aria-modal", "true");
      dialog.setAttribute("aria-label", "Diagram viewer");

      const toolbar = document.createElement("div");
      toolbar.className = "lightbox-toolbar";

      titleEl = document.createElement("div");
      titleEl.className = "lightbox-title";

      const actions = document.createElement("div");
      actions.className = "lightbox-actions";

      openLink = document.createElement("a");
      openLink.className = "lightbox-open";
      openLink.target = "_blank";
      openLink.rel = "noopener noreferrer";
      openLink.textContent = "Open SVG";

      closeBtn = document.createElement("button");
      closeBtn.type = "button";
      closeBtn.className = "lightbox-close";
      closeBtn.textContent = "Close";
      closeBtn.setAttribute("aria-label", "Close viewer");

      actions.appendChild(openLink);
      actions.appendChild(closeBtn);
      toolbar.appendChild(titleEl);
      toolbar.appendChild(actions);

      const body = document.createElement("div");
      body.className = "lightbox-body";

      zoomImg = document.createElement("img");
      zoomImg.className = "lightbox-img";
      zoomImg.alt = "";

      body.appendChild(zoomImg);
      dialog.appendChild(toolbar);
      dialog.appendChild(body);
      backdrop.appendChild(dialog);
      document.body.appendChild(backdrop);

      // Close interactions
      backdrop.addEventListener("click", (e) => {
        if (e.target === backdrop) close();
      });
      closeBtn.addEventListener("click", close);
      document.addEventListener("keydown", (e) => {
        if (!backdrop || backdrop.hidden) return;
        if (e.key === "Escape") {
          e.preventDefault();
          close();
        }
      });
    }

    function open(img) {
      ensureOverlay();
      lastActive = document.activeElement;

      const src = img.getAttribute("src") || "";
      const alt = img.getAttribute("alt") || "Diagram";

      titleEl.textContent = alt;
      zoomImg.src = src;
      zoomImg.alt = alt;
      openLink.href = src || "#";

      backdrop.hidden = false;
      document.body.classList.add("lightbox-open");
      closeBtn.focus();
    }

    function close() {
      if (!backdrop) return;
      backdrop.hidden = true;
      document.body.classList.remove("lightbox-open");
      zoomImg.removeAttribute("src");
      if (lastActive && typeof lastActive.focus === "function") lastActive.focus();
    }

    targets.forEach((img) => {
      img.classList.add("lightbox-target");
      if (!img.hasAttribute("tabindex")) img.tabIndex = 0;
      img.setAttribute("role", "button");
      img.setAttribute(
        "aria-label",
        `Open diagram${img.alt ? `: ${img.alt}` : ""}`
      );

      img.addEventListener("click", (e) => {
        // Meta/Ctrl-click should behave like "open in new tab".
        if (e.metaKey || e.ctrlKey) {
          window.open(img.src, "_blank", "noopener,noreferrer");
          return;
        }
        open(img);
      });

      img.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          open(img);
        }
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();

