import json
import sys

for f in sys.argv[1:]:
    d = json.load(open(f))
    print(
        "#####",
        f.rsplit("/", 1)[1],
        d["window"],
        "P",
        d["P"],
        "thresh",
        d["thresh"],
        "eligible",
        d["eligible_jobs"],
        "users",
        d["users_among_chosen"],
        "maxjobs/user",
        d["max_chosen_jobs_per_user"],
        "users>=10",
        d["users_with_ge10_chosen"],
        d["sclass_counts"],
    )
    for m in ("cpu", "mem"):
        r = d[m]
        print(
            " ",
            m,
            {
                k: (float("%.4g" % v) if isinstance(v, float) else v)
                for k, v in r.items()
                if not isinstance(v, dict)
            },
        )
        for g in ("random", "user_sorted"):
            print(
                "    ",
                g,
                "zipf svc %.3f region %.3f"
                % (r[g]["zipf_service"], r[g]["zipf_region"]),
            )
            for lvl in ("rs", "rsj"):
                if g == "user_sorted" and lvl == "rsj":
                    continue
                for w in ("full", "1m=600", "5m=3000"):
                    print(
                        "       %-8s" % (lvl + "/" + w),
                        "  ".join(
                            "k%d share %.3f gap %.3f (p10 %.3f, min %.3f, set==%.2f)"
                            % (
                                k,
                                *(
                                    r[g][f"{lvl}/{w}/k{k}"][x]
                                    for x in (
                                        "share_med",
                                        "gap_med",
                                        "gap_p10",
                                        "gap_min",
                                        "set_eq_full_frac",
                                    )
                                ),
                            )
                            for k in (3, 5, 8)
                        ),
                    )
