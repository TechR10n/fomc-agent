(function initCaseStudySite() {
  const header = document.querySelector(".site-header");
  const menuToggle = document.querySelector("[data-menu-toggle]");
  const mainNav = document.querySelector("[data-main-nav]");

  if (header && menuToggle && mainNav) {
    menuToggle.addEventListener("click", () => {
      const open = header.classList.toggle("nav-open");
      menuToggle.setAttribute("aria-expanded", String(open));
    });

    mainNav.querySelectorAll("a").forEach((link) => {
      link.addEventListener("click", () => {
        header.classList.remove("nav-open");
        menuToggle.setAttribute("aria-expanded", "false");
      });
    });
  }

  const currentPage = document.body.dataset.page;
  if (currentPage) {
    const activeLink = document.querySelector(`[data-nav="${currentPage}"]`);
    if (activeLink) activeLink.classList.add("active");
  }

  const nowYear = new Date().getFullYear();
  document.querySelectorAll("[data-current-year]").forEach((node) => {
    node.textContent = String(nowYear);
  });

  const shareUrl = window.location.href;
  const shareTitle = document.body.dataset.shareTitle || document.title;

  document.querySelectorAll("[data-share]").forEach((link) => {
    const kind = link.getAttribute("data-share");
    if (!kind) return;

    if (kind === "linkedin") {
      link.href = `https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent(shareUrl)}`;
      return;
    }

    if (kind === "facebook") {
      link.href = `https://www.facebook.com/sharer/sharer.php?u=${encodeURIComponent(shareUrl)}`;
      return;
    }

    if (kind === "twitter") {
      link.href = `https://twitter.com/intent/tweet?url=${encodeURIComponent(shareUrl)}&text=${encodeURIComponent(shareTitle)}`;
      return;
    }
  });

  const revealNodes = Array.from(document.querySelectorAll(".reveal"));
  if (!revealNodes.length) return;

  if (!("IntersectionObserver" in window)) {
    revealNodes.forEach((node) => node.classList.add("is-visible"));
    return;
  }

  const observer = new IntersectionObserver(
    (entries, obs) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("is-visible");
        obs.unobserve(entry.target);
      });
    },
    { rootMargin: "0px 0px -8% 0px", threshold: 0.2 }
  );

  revealNodes.forEach((node) => observer.observe(node));
})();
