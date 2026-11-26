from pathlib import Path
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.font_manager import fontManager
from matplotlib.ticker import FuncFormatter

DEST = Path(__file__).resolve().parent / "figures"
DEST.mkdir(exist_ok=True)
NAVY = "#0c213b"
BLUE = "#285374"
MID = "#4c7696"
PALE = "#a0b6c9"
GRID = "#dce5ed"
for folder in (Path("/usr/share/texmf/fonts/opentype/public/lm"),
               Path("/usr/share/texlive/texmf-dist/fonts/opentype/public/lm")):
    for name in ("lmroman10-regular.otf", "lmroman10-bold.otf"):
        if (folder / name).is_file():
            fontManager.addfont(folder / name)
mpl.rcParams.update({
    "font.family": ["Latin Modern Roman", "DejaVu Serif"], "font.size": 11,
    "text.color": NAVY, "axes.labelcolor": NAVY, "axes.edgecolor": PALE,
    "xtick.color": NAVY, "ytick.color": NAVY, "axes.spines.top": False,
    "axes.spines.right": False, "axes.axisbelow": True, "axes.grid": True,
    "grid.color": GRID, "grid.linewidth": .65, "axes.titleweight": "bold",
    "legend.frameon": False, "savefig.facecolor": "white", "pdf.fonttype": 3,
})

def plot(size=(8.3, 3.5), cols=1):
    return plt.subplots(1, cols, figsize=size, layout="constrained")

def save(fig, name):
    fig.savefig(DEST / (name + ".pdf"), bbox_inches="tight", pad_inches=.07)
    fig.savefig(DEST / (name + ".png"), dpi=210, bbox_inches="tight", pad_inches=.07)
    plt.close(fig)

def values(ax, container, fmt="%.2f", padding=3):
    ax.bar_label(container, fmt=fmt, padding=padding, fontsize=10, color=NAVY)

def style_markers():
    return [("o", "-", NAVY), ("s", "--", MID), ("^", ":", BLUE)]

# Phase 1, output 80 and storage totals in output 76.
years = [1984, 1995, 2009, 2020, 2024]
fig, ax = plot((8.3, 3.4))
for (m, ls, c), label, data in zip(style_markers(), ["CSV", "Parquet Snappy", "Parquet ZSTD"], [
    [27.579, 20.568, 14.902, 9.129, 8.943],
    [2.869, 2.484, 2.611, 2.273, 2.096],
    [3.752, 2.664, 3.215, 2.278, 2.178],
]):
    ax.plot(range(len(years)), data, marker=m, ls=ls, color=c, lw=2.1, label=label)
ax.set(xticks=range(len(years)), xticklabels=years, ylabel="Median read time (ms)", ylim=(0, 31))
ax.grid(axis="x", visible=False)
ax.legend(loc="upper right", ncol=1)
save(fig, "storage_projection")

# Phase 2, output 203. Core is the share of complete records, broad is cell availability.
fig, ax = plot((8.3, 3.25))
idx = np.arange(3)
values(ax, ax.bar(idx - .18, [97.74, 99.73, 99.71], .34, color=NAVY, label="Six core fields: complete rows"), "%.1f%%")
values(ax, ax.bar(idx + .18, [67.73, 79.63, 89.09], .34, color=MID, label="All 133 financial fields: available cells"), "%.1f%%")
ax.set(xticks=idx, xticklabels=["1984–1994", "1995–2004", "2005–2025Q2"], ylim=(0, 115), ylabel="Completeness (%)")
ax.grid(axis="x", visible=False)
ax.legend(loc="lower right", facecolor="white", frameon=True, edgecolor="white", framealpha=.94)
save(fig, "historical_coverage")

# Phase 2, output 201, zero-count years filled explicitly.
event_counts = {1984:83, 1985:139, 1986:162, 1987:217, 1988:232, 1989:530, 1990:381, 1991:268,
                1992:179, 1993:50, 1994:15, 1995:8, 1996:6, 1997:1, 1998:3, 1999:8, 2000:7, 2001:4,
                2002:11, 2003:3, 2004:4, 2007:3, 2008:25, 2009:140, 2010:157, 2011:92, 2012:51,
                2013:24, 2014:18, 2015:8, 2016:5, 2017:8, 2019:4, 2020:4, 2023:5, 2024:2, 2025:2, 2026:2}
assert sum(event_counts.get(y, 0) for y in range(1985, 2007)) == 2228
assert sum(event_counts.get(y, 0) for y in range(2008, 2013)) == 465
assert sum(event_counts.get(y, 0) for y in range(2014, 2026)) == 56
fig, ax = plot((8.3, 3.15))
x = list(range(1984, 2027))
ax.axvspan(2007.5, 2012.5, color=GRID, zorder=0)
ax.bar(x, [event_counts.get(y, 0) for y in x], color=[NAVY if 2008 <= y <= 2012 else MID for y in x], width=.76)
ax.set(ylabel="Dated failure records", ylim=(0, 590), xticks=[1984,1990,1996,2002,2008,2014,2020,2026], xlim=(1983.4, 2026.6))
ax.annotate("530 in 1989", (1989, 530), (1992, 527), arrowprops={"arrowstyle":"-", "color":MID}, color=NAVY)
ax.text(2010, 260, "Crisis test\n465 failures", ha="center", color=NAVY)
ax.grid(axis="x", visible=False)
save(fig, "dated_failures")

# Phase 2, output 412. Roll up small resolved branches without hiding retained nulls.
labels = ["Verified merger", "Unresolved administrative exit", "Financial-panel end, horizon mature",
          "Other resolved branches", "Right censored", "Pre-existing failure", "Quarantined"]
totals = [55754, 7514, 4273, 3934, 13070, 1176, 70]
assert sum(totals) == 85791
fig, ax = plot((8.3, 3.3))
p = ax.barh(labels[::-1], totals[::-1], color=([NAVY]*4+[MID]*3)[::-1], height=.63)
ax.bar_label(p, labels=[f"{v:,}" for v in totals[::-1]], padding=4, fontsize=10)
ax.set(xlim=(0, 64000), xlabel="Original V2 missing labels (bank–quarters)")
ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1000:g}k" if x else "0"))
ax.grid(axis="y", visible=False)
ax.plot([], [], color=NAVY, lw=6, label="Resolved to zero under V3")
ax.plot([], [], color=MID, lw=6, label="Remained missing under V3")
ax.legend(loc="lower right")
save(fig, "label_resolution")

# Phase 3, output 69. All runs versus a warm-tail sensitivity.
fig, ax = plot((8.3, 3.25))
idx = np.arange(3)
values(ax, ax.bar(idx - .18, [120.648,155.201,1686.024], .34, color=NAVY, label="All five runs"), "%.0f")
values(ax, ax.bar(idx + .18, [114.932,122.813,1614.948], .34, color=MID, label="Last three runs"), "%.0f")
ax.set(xticks=idx, xticklabels=["Polars lazy", "pandas", "PySpark local"], ylabel="Median latency (ms, log scale)", yscale="log", ylim=(70, 3000))
ax.set_yticks([100,200,500,1000,2000], labels=["100","200","500","1,000","2,000"])
ax.grid(axis="x", visible=False)
ax.legend(loc="upper left")
save(fig, "engine_benchmark")

# Phase 3, output 48. Matched runtimes and sampled process RSS.
fig, (ax, mem) = plot((8.3, 3.35), 2)
hist = np.array([163894,392413,1056900,1610856]) / 1e6
for target, vals, ylab in [
    (ax, [[240.77,353.26,379.91,447.12],[183.94,367.66,719.46,867.06]], "Median runtime (ms)"),
    (mem, [[1475.59,1786.44,1907.85,2190.54],[1375.21,1737.94,1852.17,2082.03]], "Sampled peak RSS (MiB)"),
]:
    for (m, ls, c), label, series in zip(style_markers(), ["Calendar window", "Four joins"], vals):
        target.plot(hist, series, marker=m, ls=ls, color=c, lw=2, label=label)
    target.set(xlabel="Output rows (millions)", ylabel=ylab, xlim=(0, 1.76))
ax.legend(loc="upper left")
save(fig, "polars_scaling")

# Phase 3 §3.9.3, matched AQE action medians.
fig, ax = plot((8.3, 3.1))
idx = np.arange(2)
values(ax, ax.bar(idx - .18,[394.35,401.09],.34,color=MID,label="AQE off"),"%.0f")
values(ax, ax.bar(idx + .18,[336.33,380.44],.34,color=NAVY,label="AQE on"),"%.0f")
ax.set(xticks=idx,xticklabels=["4 shuffle partitions","16 shuffle partitions"],ylabel="Median runtime (ms)",ylim=(0,495))
ax.grid(axis="x",visible=False)
ax.legend(loc="upper left", ncol=2)
save(fig, "spark_aqe")

# Phase 5 fold definition and Phase 2 output 430.
fig, (ax, rate) = plot((8.3, 3.5), 2)
spans = [(1987,1988,1991),(1991,1992,1995),(1996,1997,2001),(2001,2002,2005)]
for i,(stop,start,end) in enumerate(spans):
    ax.broken_barh([(1985,stop-1985)], (i-.29,.58), facecolors=NAVY)
    ax.broken_barh([(stop,start-stop)], (i-.29,.58), facecolors=GRID)
    ax.broken_barh([(start,end-start)], (i-.29,.58), facecolors=MID)
ax.set(yticks=range(4), yticklabels=["F1","F2","F3","F4"], xticks=[1985,1990,1995,2000,2005], xlabel="Forecast origin (year)", xlim=(1984.5,2005.5))
ax.invert_yaxis()
ax.grid(axis="y",visible=False)
for c,l in [(NAVY,"Train"),(GRID,"Purge"),(MID,"Validation")]:
    ax.plot([],[],lw=6,color=c,label=l)
ax.legend(loc="upper center", bbox_to_anchor=(.5,1.13),ncol=3,fontsize=9, columnspacing=.9)
vals = [2.3594,.3128,.0464,.0338]
b = rate.bar(range(4),vals,color=[NAVY,BLUE,MID,MID],width=.55)
rate.bar_label(b,labels=[f"{v:g}%" for v in vals],padding=3,fontsize=9)
rate.set(xticks=range(4),xticklabels=["F1","F2","F3","F4"],ylabel="Positive validation quarters",yscale="log",ylim=(.022,4.4))
rate.set_yticks([.03,.1,.3,1,3],labels=["0.03%","0.1%","0.3%","1%","3%"])
rate.grid(axis="x",visible=False)
save(fig, "chronological_validation")

# Phase 5 §5.4B fixed-capacity output including K=35 in the Crisis ladder.
fig, (ax, fp) = plot((8.3, 3.35),2)
for (m,ls,c), lab, k, hit, bad, n in [
    (style_markers()[0],"Crisis test",[10,25,35,50],[144,278,330,377],[25,92,152,283],487),
    (style_markers()[1],"Recent OOT",[10,25,35,50],[38,44,44,44],[372,1031,1484,2166],53),
]:
    ax.plot(k,np.array(hit)/n*100,marker=m,ls=ls,color=c,lw=2,label=lab)
    fp.plot(k,bad,marker=m,ls=ls,color=c,lw=2)
for a,y in [(ax,"Positive-bank recall (%)"),(fp,"False alerts (bank–quarters)")]:
    a.set(xticks=[10,25,35,50],xlabel="Reviews available each quarter",ylabel=y)
    a.axvline(25, color=PALE, lw=.8, ls=":")
ax.set_ylim(20,94)
ax.legend(loc="lower right",fontsize=9)
save(fig,"review_capacity")

# Phase 5 cell 322. 2021 has zero positive quarters and no AP.
fig, (ax, ot) = plot((8.3, 3.45),2)
cr = [.46240,.72157,.75994,.71358,.70508]
rr = [.55005,.34909,.64555,.41176,.04102,.44017,.75763,np.nan,.00116,.15398,.00308,.48275]
for a,x,ys,lab,t in [(ax,range(2008,2013),cr,"Crisis test",[2008,2010,2012]),
                       (ot,range(2014,2026),rr,"Recent OOT",[2014,2017,2020,2022,2025])]:
    a.plot(x,ys,marker="o",color=NAVY,lw=2)
    a.set(ylim=(0,1.06),xticks=t,title=lab,xlabel="Forecast origin (year)")
ax.set_ylabel("Recency-weighted RF AP")
ot.annotate("2021: no positives",(2021,.08),(2016.3,.13),color=BLUE,
            arrowprops={"arrowstyle":"-","color":MID},fontsize=9)
save(fig,"annual_ap")

# Phase 5 §5.5A-RF output 329.
fig, ax = plot((8.3, 3.6))
labels = ["RBC1AAJ", "RBCRWAJ", "Equity / assets", "Noncurrent loans / gross loans",
          "Noncurrent loans × profitability", "Expense burden × profitability", "Equity / assets squared"]
vv = [.13651,.11204,.10086,.08419,.06810,.06718,.06497]
values(ax,ax.barh(labels[::-1],vv[::-1],color=[MID]*5+[NAVY]*2,height=.67),"%.3f")
ax.set(xlim=(0,.158),xlabel="Mean decrease in impurity (share)")
ax.grid(axis="y",visible=False)
save(fig,"rf_importance")

# Phase 5 §5.4B, Brier comparison relative to a retrospective prevalence baseline.
fig, ax = plot((8.3, 3.25))
prev = np.array([1880/157851,175/249606])
baseline = prev*(1-prev)
idx = np.arange(2)
for i, (c,l,b) in enumerate([(MID,"Raw RF", [.014240,.002080]),(NAVY,"Calibrated RF",[.007335,.000786])]):
    skill = (1-np.array(b)/baseline)*100
    patches = ax.bar(idx+(-.18 if i==0 else .18),skill,width=.34,color=c,label=l)
    values(ax,patches,"%.1f%%",padding=3)
ax.axhline(0,color=PALE,lw=1)
ax.set(xticks=idx,xticklabels=["Crisis test","Recent OOT"],ylabel="Brier skill vs. observed prevalence (%)",ylim=(-218,65))
ax.grid(axis="x",visible=False)
ax.legend(loc="lower left")
save(fig,"calibration_skill")

# Added diagrams and archived precision–recall rendering for the outlined report.
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle
from PIL import Image

fig, ax = plot((8.5, 2.30))
ax.set(xlim=(0, 9.25), ylim=(0, 2.85))
ax.axis("off")
boxes = [
    (.05, 1.72, "FDIC API + Raw", "170 quarters\n1,682,616 financial rows"),
    (3.15, 1.72, "Typed Bronze", "43 annual files\n136 source fields"),
    (6.25, 1.72, "Silver and V3", "Financial audits +\nseparate outcomes"),
    (6.25, .19, "W1", "1,610,856 keyed rows\n9 core features"),
    (3.15, .19, "Gold", "1,121,901 TRAIN rows\n8,239 positive quarters"),
    (.05, .19, "Phase 5", "Train-only fit and freeze\nthen external scoring"),
]
for x, y, name, desc in boxes:
    ax.add_patch(FancyBboxPatch((x, y), 2.84, .94, boxstyle="round,pad=0.04,rounding_size=.09",
                                facecolor="#edf3f8", edgecolor="#7593ac", lw=.8))
    ax.text(x + 1.42, y+.67, name, ha="center", color=NAVY, fontweight="bold", fontsize=14)
    ax.text(x + 1.42, y+.21, desc, ha="center", color=NAVY, fontsize=11.4)
for st, en in [((2.92,2.19),(3.12,2.19)),((6.02,2.19),(6.22,2.19)),
               ((7.67,1.65),(7.67,1.18)),((6.2,.66),(6.02,.66)),((3.1,.66),(2.92,.66))]:
    ax.add_patch(FancyArrowPatch(st,en,arrowstyle="-|>",mutation_scale=10,color=NAVY,lw=1.1))
save(fig,"pipeline_architecture")

fig, ax = plot((8.5, 2.05))
ax.set(xlim=(-.46, 5.42), ylim=(-.62,1.30))
ax.axis("off")
ax.add_patch(Rectangle((.28,.20), 3.99,.54, color="#edf3f8"))
ax.hlines(.48,0,5,color=BLUE,lw=1.25)
for i,label in enumerate(["2020Q4","2021Q1","2021Q2","2021Q3","2021Q4","2022Q1"]):
    ax.plot(i,.48,"o",color=NAVY if i in [0,4] else MID,ms=7 if i in [0,4] else 4.5)
    ax.text(i,.1,label,ha="center",fontsize=12)
ax.text(0,-.27,"Origin excluded",ha="center",color=NAVY,fontsize=11)
ax.text(4,-.27,"Horizon end included",ha="center",color=NAVY,fontsize=11)
ax.text(5,.98,"Outside window",ha="center",color=NAVY,fontsize=11)
ax.text(2.25,.97,"Eligible failure dates after origin through quarter 4",ha="center",color=NAVY,fontsize=12,fontweight="bold")
save(fig,"label_timeline")

# Crop only the current notebook axes, retaining its grid and data traces. The old
# legend is outside the traces at that location. No underlying scores are
# inferred from the raster or used to recalculate AP.
source = Path(__file__).resolve().parent / "recent_rf_notebook_plots.png"
original = np.asarray(Image.open(source).convert("RGB"))
curve = original[34:435, 63:630].copy()
curve[5:55, 214:562] = 255  # Replace old legend with an English legend.
rgb = curve.astype(float)
for series, destination in [
    ((53, 97, 141), (12,33,59)),
    ((200,115,52), (76,118,150)),
]:
    old=np.asarray(series,dtype=float)
    delta=255-old
    alpha=np.clip(np.sum((255-rgb)*delta,axis=-1)/np.sum(delta**2),0,1)
    reconstruction=255-alpha[...,None]*delta
    colored=(np.max(np.abs(rgb-reconstruction),axis=-1)<9)&(np.ptp(rgb,axis=-1)>14)
    rgb[colored]=255-alpha[colored,None]*(255-np.asarray(destination))
fig, ax = plot((8.5,3.55))
ax.grid(False)
ax.imshow(rgb.astype(np.uint8),extent=(0,1,0,1.018),origin="upper",aspect="auto",interpolation="nearest")
ax.set(xlim=(0,1),ylim=(0,1.018),xlabel="Bank–quarter recall",ylabel="Bank–quarter precision")
for color,label in [(NAVY,"Crisis (AP 0.63361)"),(MID,"Recent OOT through 2025Q2 (AP 0.37201)")]:
    ax.plot([],[],color=color,lw=2.5,label=label)
ax.legend(loc="upper right",frameon=True,facecolor="white")
save(fig,"current_rf_pr")

# The saved notebook PNG is the evidence for the 40-bin histograms. Retain the
# plotted pixels and their positions, and redraw the typography and legend.
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

boot = np.asarray(Image.open(DEST.parent / "validation_bootstrap.png").convert("RGB"))
panels = [
    ("Mean AP difference", (67, 64, 680, 408), (71, 68, 227, 122), 263,
     ([164, 263, 363, 463, 563, 663], ["−0.005", "0", "0.005", "0.010", "0.015", "0.020"]),
     ([408, 329, 250, 171, 92], [0, 40, 80, 120, 160])),
    ("Bank recall@25 difference (pp)", (767, 64, 1380, 408), (1220, 68, 1376, 122), 838,
     ([838, 910, 983, 1055, 1128, 1200, 1272, 1344], ["0", "0.2", "0.4", "0.6", "0.8", "1.0", "1.2", "1.4"]),
     ([408, 327, 246, 165, 84], [0, 40, 80, 120, 160])),
    ("Additional detected bank–folds", (67, 489, 680, 833), (71, 493, 227, 546), 122,
     ([122, 261, 401, 541, 680], [0, 5, 10, 15, 20]),
     ([833, 774, 715, 656, 597, 537], [0, 50, 100, 150, 200, 250])),
    ("Reduction in false alerts@25", (767, 489, 1380, 833), (1220, 493, 1376, 546), 905,
     ([826, 905, 985, 1065, 1144, 1224, 1304], ["−5", "0", "5", "10", "15", "20", "25"]),
     ([833, 748, 663, 578, 492], [0, 40, 80, 120, 160])),
]

fig, axes = plt.subplots(2, 2, figsize=(8.6, 5.48))
fig.subplots_adjust(left=.08, right=.985, bottom=.15, top=.86, wspace=.18, hspace=.36)
for ax, (title, bounds, legend, zero_x, xticks, yticks) in zip(axes.flat, panels):
    left, top, right, bottom = bounds
    x1, y1, x2, y2 = legend
    pixels = boot[top:bottom, left:right].astype(float)
    # The old legends occupy empty space above the bars. Extend the color
    # band and vertical grid from below each box. Continue horizontal grid
    # lines using the unobstructed white-background column beside the box.
    reference_x = x2 + 1
    row_offset = pixels[y1-top:y2-top, reference_x-left] - boot[y2, reference_x]
    pixels[y1-top:y2-top, x1-left:x2-left] = boot[y2, x1:x2] + row_offset[:, None, :]

    red, green, blue = np.moveaxis(pixels, -1, 0).copy()
    orange = (red-green > 7) & (green-blue > 4)
    orange_strength = np.clip((red-blue) / 148, 0, 1)
    pixels[orange] = 255 - orange_strength[orange, None] * (255 - np.array([12, 33, 59]))
    blue_tones = ~orange & (blue-red > 1.5)
    blue_strength = np.clip((blue-red) / 77, 0, 1)
    palette_shift = np.array([-19, -22, -24])
    pixels[blue_tones] += blue_strength[blue_tones, None] * palette_shift

    # Replace the archived dash at the same x-position, including any
    # portion that was covered by the original legend.
    zero_strip = pixels[:, zero_x-left-1:zero_x-left+2]
    zero_ink = np.max(zero_strip, axis=2) < 85
    adjacent = pixels[:, [zero_x-left-2, zero_x-left+2]].mean(axis=1)
    zero_strip[:] = np.where(zero_ink[..., None], adjacent[:, None, :], zero_strip)

    ax.grid(False)
    ax.imshow(pixels.clip(0, 255).astype(np.uint8), extent=(left, right, bottom, top),
              origin="upper", aspect="auto", interpolation="none")
    ax.axvline(zero_x, color=NAVY, linewidth=.9, linestyle="--")
    ax.set(xlim=(left, right), ylim=(bottom, top), title=title)
    ax.set_xticks(*xticks)
    ax.set_yticks(*yticks)
    ax.tick_params(length=2.5, labelsize=10)

fig.suptitle("Paired block bootstrap on validation OOF", x=.08, y=.985,
             ha="left", fontsize=15, fontweight="bold")
fig.supylabel("Bootstrap frequency", x=.011, y=.49, fontsize=11)
fig.supxlabel("Improvement of recency-weighted RF", y=.066, fontsize=11)
fig.legend(handles=[
    Patch(facecolor="#e2ebf1", label="95% bootstrap interval"),
    Line2D([], [], color=NAVY, linestyle="--", label="No difference"),
    Line2D([], [], color=NAVY, linewidth=2, label="Observed difference"),
], loc="lower center", bbox_to_anchor=(.52, -.015), ncol=3, fontsize=10)
save(fig, "validation_bootstrap")

def compact_archive_bars(name, labels, counts, colors, left, height):
    fig, ax = plt.subplots(figsize=(6.65, height))
    fig.subplots_adjust(left=left, right=.99, top=.97, bottom=.04)
    positions = np.arange(len(labels))
    bars = ax.barh(positions, counts, height=.62, color=colors)
    ax.bar_label(bars, labels=[f"{n:,}" for n in counts], padding=4,
                 fontsize=9.3, color=NAVY)
    ax.set_yticks(positions, labels, fontsize=9.3)
    ax.set_xlim(0, max(counts)*1.18)
    ax.set_ylim(len(labels)-.49, -.51)
    ax.set_xticks([])
    ax.tick_params(axis="y", length=0, pad=5)
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    save(fig, name)


# Phase 1, cell 47: displayed checkpoints and the archive maximum.
compact_archive_bars(
    "archive_quarter_counts",
    ["1984Q1", "1986Q1 (peak)", "2005Q2", "2026Q2"],
    [17930, 18133, 8973, 4313],
    [MID, NAVY, MID, MID],
    left=.20, height=.92,
)

# Phase 2, cell 308: accepted failures, accepted assistance and quarantine reasons.
compact_archive_bars(
    "registry_validation_counts",
    ["Valid failure CERTs", "Assistance events",
     "Missing CERT", "Conflicting failure dates"],
    [3033, 590, 488, 6],
    [NAVY, BLUE, MID, MID],
    left=.30, height=1.18,
)
